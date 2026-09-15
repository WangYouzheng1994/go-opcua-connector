package collector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go-opcua-connector/internal/config"

	"go.uber.org/zap"
)

const defaultAcquisitionStopTimeout = 10 * time.Second

var errSourceConnectionLost = errors.New("source connection lost during recovery")

type acquisitionRuntimeConfig struct {
	verificationInterval time.Duration
	staleThreshold       time.Duration
	retryInterval        time.Duration
	connectionInterval   time.Duration
	recoveryBackoff      []time.Duration
	readTimeout          time.Duration
	readBatchSize        int
	staleCheckInterval   time.Duration
	mismatchRecheckDelay time.Duration
}

// AcquisitionCoordinator 协调状态池、订阅、主动验证、单点重建和断线恢复。
type AcquisitionCoordinator struct {
	adapter SourceAdapter
	store   StateStore
	config  acquisitionRuntimeConfig
	logger  *zap.Logger
	now     func() time.Time

	lifecycleMu  sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	started      bool
	stopped      bool
	wg           sync.WaitGroup
	operationMu  sync.Mutex
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error

	connectionMu         sync.RWMutex
	connectionGeneration uint64
	sourceMu             sync.RWMutex
	sourceAvailable      bool

	subscriptionsMu sync.Mutex
	subscriptions   []PointSubscription
	ownerByPoint    map[PointID]PointSubscription

	rebuildWake chan struct{}
	rebuildMu   sync.Mutex
	queued      map[PointID]struct{}

	discoveryMu      sync.Mutex
	discoveryPending bool
}

// NewAcquisitionCoordinator 创建采集协调器。
func NewAcquisitionCoordinator(adapter SourceAdapter, store StateStore, cfg config.CollectorConfig, logger *zap.Logger) (*AcquisitionCoordinator, error) {
	if adapter == nil {
		return nil, errors.New("source adapter is required")
	}
	if store == nil {
		return nil, errors.New("state store is required")
	}
	if cfg.HeartbeatIntervalSec <= 0 || cfg.StaleThresholdSec <= 0 || cfg.SubscriptionRetryIntervalSec <= 0 || cfg.ReadTimeoutSec <= 0 || cfg.ReadBatchSize <= 0 {
		return nil, errors.New("acquisition timing and batch configuration must be positive")
	}
	return &AcquisitionCoordinator{
		adapter: adapter,
		store:   store,
		config: acquisitionRuntimeConfig{
			verificationInterval: time.Duration(cfg.HeartbeatIntervalSec) * time.Second,
			staleThreshold:       time.Duration(cfg.StaleThresholdSec) * time.Second,
			retryInterval:        time.Duration(cfg.SubscriptionRetryIntervalSec) * time.Second,
			connectionInterval:   time.Second,
			recoveryBackoff:      []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second},
			readTimeout:          time.Duration(cfg.ReadTimeoutSec) * time.Second,
			readBatchSize:        cfg.ReadBatchSize,
			staleCheckInterval:   time.Second,
			mismatchRecheckDelay: time.Second,
		},
		logger:               logger,
		now:                  time.Now,
		connectionGeneration: 1,
		sourceAvailable:      true,
		ownerByPoint:         make(map[PointID]PointSubscription),
		rebuildWake:          make(chan struct{}, 1),
		queued:               make(map[PointID]struct{}),
		shutdownDone:         make(chan struct{}),
	}, nil
}

// Start 完成首次发现、订阅和初读后启动后台协调任务。
func (c *AcquisitionCoordinator) Start(parent context.Context) error {
	c.lifecycleMu.Lock()
	if c.started {
		c.lifecycleMu.Unlock()
		return errors.New("acquisition coordinator already started")
	}
	c.ctx, c.cancel = context.WithCancel(parent)
	c.started = true
	c.lifecycleMu.Unlock()

	connectionGeneration := c.currentConnectionGeneration()
	discovery := c.adapter.Discover(c.ctx, connectionGeneration)
	c.logDiscoveryIssues(discovery)
	c.setDiscoveryPending(len(discovery.Issues) > 0)
	points := uniquePointIDs(discovery.Points)
	if len(points) == 0 {
		return c.failStart(discoveryFailure("source discovery returned no points", discovery))
	}
	tokens := initialTokens(points, connectionGeneration)
	if err := c.store.Initialize(points, tokens); err != nil {
		return c.failStart(fmt.Errorf("initialize state store: %w", err))
	}

	subscription := c.adapter.Subscribe(c.ctx, points, tokens, func(sample PointSample) {
		c.store.ApplySubscription(sample)
	})
	succeeded := make(map[PointID]struct{}, len(subscription.Succeeded))
	if subscription.Subscription != nil {
		c.trackSubscription(subscription.Subscription)
	}
	for _, pointID := range subscription.Succeeded {
		if _, exists := tokens[pointID]; !exists || subscription.Subscription == nil {
			continue
		}
		succeeded[pointID] = struct{}{}
		c.setOwner(pointID, subscription.Subscription)
	}
	for _, failure := range subscription.Failed {
		if token, exists := tokens[failure.PointID]; exists {
			c.store.MarkPointFailed(failure.PointID, token, PointSubscribeFailed, failure.Reason)
		}
	}
	for _, pointID := range points {
		if _, ok := succeeded[pointID]; !ok {
			c.store.MarkPointFailed(pointID, tokens[pointID], PointSubscribeFailed, "subscription did not create a monitored item")
		}
	}

	c.readInitial(c.ctx, points)
	valid := 0
	for pointID := range succeeded {
		snapshot, ok := c.store.Get(pointID)
		if ok && snapshot.HasValue && snapshot.Validity == SampleValid && snapshot.Health == PointHealthy {
			valid++
		}
	}
	if valid == 0 {
		return c.failStart(errors.New("no point has both a working subscription and a valid initial value"))
	}

	c.wg.Add(4)
	go c.verificationLoop()
	go c.staleLoop()
	go c.retryLoop()
	go c.connectionLoop()
	return nil
}

func (c *AcquisitionCoordinator) failStart(startErr error) error {
	c.lifecycleMu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	c.stopped = true
	c.lifecycleMu.Unlock()
	closeCtx, cancel := context.WithTimeout(context.Background(), defaultAcquisitionStopTimeout)
	defer cancel()
	closeErr := c.closeSubscriptions(closeCtx)
	c.shutdownOnce.Do(func() {
		c.shutdownErr = closeErr
		close(c.shutdownDone)
	})
	return errors.Join(startErr, closeErr)
}

// Stop 按“取消、关闭订阅、等待后台任务”的顺序幂等停止协调器。
// 正常停止不关闭共享 SourceAdapter；其生命周期仍由应用入口负责。
func (c *AcquisitionCoordinator) Stop(ctx context.Context) error {
	c.lifecycleMu.Lock()
	if !c.started {
		c.lifecycleMu.Unlock()
		return nil
	}
	if !c.stopped {
		c.stopped = true
		c.cancel()
	}
	c.lifecycleMu.Unlock()

	stopCtx, cancel := acquisitionStopContext(ctx)
	defer cancel()
	c.shutdownOnce.Do(func() { go c.finishShutdown() })
	select {
	case <-c.shutdownDone:
		return c.shutdownErr
	case <-stopCtx.Done():
		return stopCtx.Err()
	}
}

func (c *AcquisitionCoordinator) finishShutdown() {
	// 实际清理只执行一次；Stop 调用方通过 shutdownDone 在自己的 deadline 内等待。
	c.operationMu.Lock()
	closeCtx, cancel := context.WithTimeout(context.Background(), defaultAcquisitionStopTimeout)
	closeErr := c.closeSubscriptions(closeCtx)
	cancel()
	c.operationMu.Unlock()
	c.wg.Wait()
	c.shutdownErr = closeErr
	close(c.shutdownDone)
}

func acquisitionStopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultAcquisitionStopTimeout)
}

func (c *AcquisitionCoordinator) readInitial(ctx context.Context, points []PointID) {
	for _, batch := range pointBatches(points, c.config.readBatchSize) {
		versions, tokens := c.capture(batch)
		batchCtx, cancel := context.WithTimeout(ctx, c.config.readTimeout)
		result := c.adapter.Verify(batchCtx, batch, tokens)
		cancel()
		if result.RequestFailure != "" {
			c.logWarn("Initial read request failed", zap.String("reason", result.RequestFailure))
			continue
		}
		for _, verification := range result.Verifications {
			expectedVersion, ok := versions[verification.PointID]
			if !ok {
				continue
			}
			current, exists := c.store.Get(verification.PointID)
			if !exists || current.Version != expectedVersion || current.HasValue {
				continue
			}
			if !c.store.SeedInitialValue(verification, expectedVersion) {
				c.store.ApplyVerification(verification, expectedVersion)
			}
		}
		c.applyReadFailures(result.Failed, versions, tokens)
	}
}

func (c *AcquisitionCoordinator) verificationLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.verificationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.runVerificationRound(c.ctx)
		}
	}
}

func (c *AcquisitionCoordinator) runVerificationRound(ctx context.Context) {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	if !c.isSourceAvailable() || ctx.Err() != nil {
		return
	}
	points := snapshotPointIDs(c.store.Snapshot())
	mismatches := make([]PointSnapshot, 0)
	for _, batch := range pointBatches(points, c.config.readBatchSize) {
		versions, tokens := c.capture(batch)
		batchCtx, cancel := context.WithTimeout(ctx, c.config.readTimeout)
		result := c.adapter.Verify(batchCtx, batch, tokens)
		cancel()
		if result.RequestFailure != "" {
			c.logWarn("Verification read request failed", zap.String("reason", result.RequestFailure))
			continue
		}
		for _, verification := range result.Verifications {
			expectedVersion, ok := versions[verification.PointID]
			if !ok || !c.store.ApplyVerification(verification, expectedVersion) {
				continue
			}
			if snapshot, exists := c.store.Get(verification.PointID); exists && snapshot.Health == PointSubscriptionMismatch {
				mismatches = append(mismatches, snapshot)
			}
		}
		c.applyReadFailures(result.Failed, versions, tokens)
	}
	c.recheckMismatches(ctx, mismatches)
}

func (c *AcquisitionCoordinator) recheckMismatches(ctx context.Context, mismatches []PointSnapshot) {
	if len(mismatches) == 0 {
		return
	}
	timer := time.NewTimer(c.config.mismatchRecheckDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	for _, recorded := range mismatches {
		current, ok := c.store.Get(recorded.PointID)
		if !ok || current.Version != recorded.Version || current.Health != PointSubscriptionMismatch {
			continue
		}
		token := tokenFromSnapshot(current)
		readCtx, cancel := context.WithTimeout(ctx, c.config.readTimeout)
		result := c.adapter.Verify(readCtx, []PointID{current.PointID}, map[PointID]GenerationToken{current.PointID: token})
		cancel()
		if result.RequestFailure != "" {
			continue
		}
		for _, verification := range result.Verifications {
			if verification.PointID != current.PointID || !c.store.ApplyVerification(verification, current.Version) {
				continue
			}
			after, exists := c.store.Get(current.PointID)
			if exists && after.Health == PointSubscriptionMismatch {
				c.enqueueRebuild(after.PointID)
			}
		}
		c.applyReadFailures(result.Failed, map[PointID]uint64{current.PointID: current.Version}, map[PointID]GenerationToken{current.PointID: token})
	}
}

func (c *AcquisitionCoordinator) staleLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.staleCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.markStale(c.now())
		}
	}
}

func (c *AcquisitionCoordinator) markStale(now time.Time) {
	for _, snapshot := range c.store.Snapshot() {
		if !staleCanChangeHealth(snapshot.Health) {
			continue
		}
		baseline := snapshot.LastConfirmedAt
		if baseline.IsZero() {
			baseline = snapshot.InitializedAt
		}
		if baseline.IsZero() || now.Sub(baseline) <= c.config.staleThreshold {
			continue
		}
		c.store.MarkPointFailedIfVersion(snapshot.PointID, tokenFromSnapshot(snapshot), snapshot.Version, PointStale, "point confirmation expired")
	}
}

func (c *AcquisitionCoordinator) retryLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.rebuildWake:
			if c.isSourceAvailable() {
				c.drainRebuilds(c.ctx)
			}
		case <-ticker.C:
			if !c.isSourceAvailable() {
				continue
			}
			if c.isDiscoveryPending() {
				c.retryDiscovery(c.ctx)
			}
			for _, snapshot := range c.store.Snapshot() {
				if snapshot.Health == PointSubscribeFailed {
					c.enqueueRebuild(snapshot.PointID)
				}
			}
		}
	}
}

func (c *AcquisitionCoordinator) retryDiscovery(ctx context.Context) {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	if !c.isSourceAvailable() || ctx.Err() != nil {
		return
	}
	c.retryDiscoveryLocked(ctx)
}

func (c *AcquisitionCoordinator) retryDiscoveryLocked(ctx context.Context) {
	connectionGeneration := c.currentConnectionGeneration()
	result := c.adapter.Discover(ctx, connectionGeneration)
	c.logDiscoveryIssues(result)
	c.setDiscoveryPending(len(result.Issues) > 0)
	existing := make(map[PointID]struct{})
	for _, snapshot := range c.store.Snapshot() {
		existing[snapshot.PointID] = struct{}{}
	}
	newPoints := make([]PointID, 0)
	for _, pointID := range uniquePointIDs(result.Points) {
		if _, exists := existing[pointID]; !exists {
			newPoints = append(newPoints, pointID)
		}
	}
	if len(newPoints) == 0 {
		return
	}
	tokens := initialTokens(newPoints, connectionGeneration)
	if err := c.store.Initialize(newPoints, tokens); err != nil {
		c.logWarn("Initialize newly discovered points failed", zap.Error(err))
		return
	}
	subscription := c.adapter.Subscribe(ctx, newPoints, tokens, func(sample PointSample) {
		c.store.ApplySubscription(sample)
	})
	if subscription.Subscription != nil {
		c.trackSubscription(subscription.Subscription)
	}
	succeeded := make(map[PointID]struct{}, len(subscription.Succeeded))
	for _, pointID := range subscription.Succeeded {
		if _, exists := tokens[pointID]; exists && subscription.Subscription != nil {
			succeeded[pointID] = struct{}{}
			c.setOwner(pointID, subscription.Subscription)
		}
	}
	for _, failure := range subscription.Failed {
		if token, exists := tokens[failure.PointID]; exists {
			c.store.MarkPointFailed(failure.PointID, token, PointSubscribeFailed, failure.Reason)
		}
	}
	for _, pointID := range newPoints {
		if _, ok := succeeded[pointID]; !ok {
			c.store.MarkPointFailed(pointID, tokens[pointID], PointSubscribeFailed, "subscription did not create a monitored item")
		}
	}
	c.readInitial(ctx, newPoints)
}

func (c *AcquisitionCoordinator) rebuildPoint(ctx context.Context, pointID PointID) {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	if !c.isSourceAvailable() || ctx.Err() != nil {
		return
	}
	c.rebuildPointLocked(ctx, pointID)
}

func (c *AcquisitionCoordinator) rebuildPointLocked(ctx context.Context, pointID PointID) {
	snapshot, ok := c.store.Get(pointID)
	if !ok || snapshot.Health != PointSubscribeFailed && snapshot.Health != PointSubscriptionMismatch {
		return
	}
	oldToken := tokenFromSnapshot(snapshot)
	newToken, ok := c.store.AdvanceMonitor(pointID, oldToken, snapshot.Health)
	if !ok {
		return
	}
	c.store.MarkPointMonitoring(pointID, newToken)
	if owner := c.owner(pointID); owner != nil {
		removeCtx, cancel := context.WithTimeout(ctx, c.config.readTimeout)
		if err := owner.Remove(removeCtx, pointID); err != nil {
			c.logWarn("Remove old monitored item failed", zap.String("point_id", string(pointID)), zap.Error(err))
		} else {
			c.clearOwner(pointID)
		}
		cancel()
	}

	result := c.adapter.Subscribe(ctx, []PointID{pointID}, map[PointID]GenerationToken{pointID: newToken}, func(sample PointSample) {
		c.store.ApplySubscription(sample)
	})
	subscribed := false
	if result.Subscription != nil {
		defer func() {
			if !subscribed {
				closeCtx, cancel := context.WithTimeout(context.Background(), c.config.readTimeout)
				_ = result.Subscription.Close(closeCtx)
				cancel()
			}
		}()
	}
	for _, succeeded := range result.Succeeded {
		if succeeded == pointID && result.Subscription != nil {
			subscribed = true
			c.setOwner(pointID, result.Subscription)
			break
		}
	}
	if !subscribed {
		reason := failureReason(result.Failed, pointID, "subscription retry failed")
		c.store.MarkPointFailed(pointID, newToken, PointSubscribeFailed, reason)
		return
	}
	c.trackSubscription(result.Subscription)

	current, ok := c.store.Get(pointID)
	if !ok {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, c.config.readTimeout)
	verification := c.adapter.Verify(readCtx, []PointID{pointID}, map[PointID]GenerationToken{pointID: newToken})
	cancel()
	if verification.RequestFailure != "" {
		return
	}
	for _, result := range verification.Verifications {
		if result.PointID != pointID {
			continue
		}
		if !current.HasValue && c.store.SeedInitialValue(result, current.Version) {
			continue
		}
		c.store.ApplyVerification(result, current.Version)
	}
	c.applyReadFailures(verification.Failed, map[PointID]uint64{pointID: current.Version}, map[PointID]GenerationToken{pointID: newToken})
}

func (c *AcquisitionCoordinator) applyReadFailures(failures []PointFailure, versions map[PointID]uint64, tokens map[PointID]GenerationToken) {
	for _, failure := range failures {
		version, versionOK := versions[failure.PointID]
		token, tokenOK := tokens[failure.PointID]
		if !versionOK || !tokenOK {
			continue
		}
		snapshot, ok := c.store.Get(failure.PointID)
		if !ok || !verificationCanChangeHealth(snapshot.Health) {
			continue
		}
		c.store.MarkPointFailedIfVersion(failure.PointID, token, version, PointVerificationFailed, failure.Reason)
	}
}

func (c *AcquisitionCoordinator) capture(points []PointID) (map[PointID]uint64, map[PointID]GenerationToken) {
	versions := make(map[PointID]uint64, len(points))
	tokens := make(map[PointID]GenerationToken, len(points))
	for _, pointID := range points {
		if snapshot, ok := c.store.Get(pointID); ok {
			versions[pointID] = snapshot.Version
			tokens[pointID] = tokenFromSnapshot(snapshot)
		}
	}
	return versions, tokens
}

func (c *AcquisitionCoordinator) enqueueRebuild(pointID PointID) {
	if c.ctx == nil || c.ctx.Err() != nil {
		return
	}
	c.rebuildMu.Lock()
	if c.ctx.Err() != nil {
		c.rebuildMu.Unlock()
		return
	}
	if _, exists := c.queued[pointID]; exists {
		c.rebuildMu.Unlock()
		return
	}
	c.queued[pointID] = struct{}{}
	c.rebuildMu.Unlock()
	select {
	case c.rebuildWake <- struct{}{}:
	default:
	}
}

func (c *AcquisitionCoordinator) drainRebuilds(ctx context.Context) {
	for ctx.Err() == nil {
		pointID, ok := c.nextRebuild()
		if !ok {
			return
		}
		c.rebuildPoint(ctx, pointID)
		c.finishRebuild(pointID)
	}
}

func (c *AcquisitionCoordinator) nextRebuild() (PointID, bool) {
	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()
	for pointID := range c.queued {
		return pointID, true
	}
	return "", false
}

func (c *AcquisitionCoordinator) finishRebuild(pointID PointID) {
	c.rebuildMu.Lock()
	delete(c.queued, pointID)
	c.rebuildMu.Unlock()
}

func (c *AcquisitionCoordinator) trackSubscription(subscription PointSubscription) {
	c.subscriptionsMu.Lock()
	c.subscriptions = append(c.subscriptions, subscription)
	c.subscriptionsMu.Unlock()
}

func (c *AcquisitionCoordinator) closeSubscriptions(ctx context.Context) error {
	c.subscriptionsMu.Lock()
	subscriptions := append([]PointSubscription(nil), c.subscriptions...)
	c.subscriptions = nil
	c.ownerByPoint = make(map[PointID]PointSubscription)
	c.subscriptionsMu.Unlock()
	var closeErrors []error
	failed := make([]PointSubscription, 0)
	for _, subscription := range subscriptions {
		if err := subscription.Close(ctx); err != nil {
			closeErrors = append(closeErrors, err)
			failed = append(failed, subscription)
		}
	}
	if len(failed) > 0 {
		c.subscriptionsMu.Lock()
		c.subscriptions = append(c.subscriptions, failed...)
		c.subscriptionsMu.Unlock()
	}
	return errors.Join(closeErrors...)
}

func (c *AcquisitionCoordinator) setOwner(pointID PointID, subscription PointSubscription) {
	c.subscriptionsMu.Lock()
	c.ownerByPoint[pointID] = subscription
	c.subscriptionsMu.Unlock()
}

func (c *AcquisitionCoordinator) owner(pointID PointID) PointSubscription {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()
	return c.ownerByPoint[pointID]
}

func (c *AcquisitionCoordinator) clearOwner(pointID PointID) {
	c.subscriptionsMu.Lock()
	delete(c.ownerByPoint, pointID)
	c.subscriptionsMu.Unlock()
}

func (c *AcquisitionCoordinator) logWarn(message string, fields ...zap.Field) {
	if c.logger != nil {
		c.logger.Warn(message, fields...)
	}
}

func (c *AcquisitionCoordinator) logInfo(message string, fields ...zap.Field) {
	if c.logger != nil {
		c.logger.Info(message, fields...)
	}
}

func (c *AcquisitionCoordinator) logDiscoveryIssues(result DiscoveryResult) {
	for _, issue := range result.Issues {
		c.logWarn("Source discovery issue", zap.String("source", issue.Source), zap.String("reason", issue.Reason))
	}
}

// discoveryFailure 保留源端诊断，避免启动或恢复失败只剩下空点表这一结果。
func discoveryFailure(message string, result DiscoveryResult) error {
	failures := []error{errors.New(message)}
	for _, issue := range result.Issues {
		if issue.Source == "" {
			failures = append(failures, errors.New(issue.Reason))
		} else {
			failures = append(failures, fmt.Errorf("%s: %s", issue.Source, issue.Reason))
		}
	}
	return errors.Join(failures...)
}

func (c *AcquisitionCoordinator) setDiscoveryPending(pending bool) {
	c.discoveryMu.Lock()
	c.discoveryPending = pending
	c.discoveryMu.Unlock()
}

func (c *AcquisitionCoordinator) isDiscoveryPending() bool {
	c.discoveryMu.Lock()
	defer c.discoveryMu.Unlock()
	return c.discoveryPending
}

func (c *AcquisitionCoordinator) currentConnectionGeneration() uint64 {
	c.connectionMu.RLock()
	defer c.connectionMu.RUnlock()
	return c.connectionGeneration
}

// advanceConnectionGeneration 是连接监督推进连接代次的唯一协调边界。
func (c *AcquisitionCoordinator) advanceConnectionGeneration(expected uint64, reason string) (uint64, bool) {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	if c.connectionGeneration != expected {
		return 0, false
	}
	next, ok := c.store.AdvanceConnection(expected, reason)
	if !ok {
		return 0, false
	}
	c.connectionGeneration = next
	return next, true
}

func (c *AcquisitionCoordinator) connectionLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.connectionInterval)
	defer ticker.Stop()
	var nextRecovery time.Time
	backoffIndex := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		case now := <-ticker.C:
			c.handleConnectionState(c.ctx, c.adapter.ConnectionState(), now, &nextRecovery, &backoffIndex)
		}
	}
}

func (c *AcquisitionCoordinator) handleConnectionState(
	ctx context.Context,
	state SourceConnectionState,
	now time.Time,
	nextRecovery *time.Time,
	backoffIndex *int,
) {
	if ctx.Err() != nil {
		return
	}
	if state != SourceConnected {
		if c.markSourceUnavailable("source connection is " + string(state)) {
			c.logWarn("Source connection unavailable",
				zap.String("source_state", string(state)),
				zap.Uint64("connection_generation", c.currentConnectionGeneration()))
			*nextRecovery = time.Time{}
			*backoffIndex = 0
		}
		return
	}
	if c.isSourceAvailable() || !nextRecovery.IsZero() && now.Before(*nextRecovery) {
		return
	}

	generation := c.currentConnectionGeneration()
	c.logInfo("Acquisition recovery started", zap.Uint64("connection_generation", generation))
	err := c.recoverConnection(ctx)
	if err == nil {
		c.setSourceAvailable(true)
		*nextRecovery = time.Time{}
		*backoffIndex = 0
		healthStats := summarizePointHealth(c.store.Snapshot())
		fields := append([]zap.Field{zap.Uint64("connection_generation", generation)}, pointHealthLogFields(healthStats)...)
		if healthStats.UnhealthyPoints > 0 {
			c.logWarn("Acquisition recovery completed with unhealthy points", fields...)
		} else {
			c.logInfo("Acquisition recovery succeeded", fields...)
		}
		return
	}
	if errors.Is(err, errSourceConnectionLost) || c.adapter.ConnectionState() != SourceConnected {
		c.logWarn("Acquisition recovery failed",
			zap.Error(err),
			zap.String("source_state", string(c.adapter.ConnectionState())))
		current := c.currentConnectionGeneration()
		c.advanceConnectionGeneration(current, "source connection failed during recovery")
		*nextRecovery = time.Time{}
		*backoffIndex = 0
		return
	}
	index := *backoffIndex
	if index >= len(c.config.recoveryBackoff) {
		index = len(c.config.recoveryBackoff) - 1
	}
	delay := c.config.recoveryBackoff[index]
	*nextRecovery = now.Add(delay)
	c.logWarn("Acquisition recovery failed", zap.Error(err), zap.Duration("retry_in", delay))
	if *backoffIndex < len(c.config.recoveryBackoff)-1 {
		*backoffIndex++
	}
}

func (c *AcquisitionCoordinator) markSourceUnavailable(reason string) bool {
	c.sourceMu.Lock()
	if !c.sourceAvailable {
		c.sourceMu.Unlock()
		return false
	}
	c.sourceAvailable = false
	c.sourceMu.Unlock()
	current := c.currentConnectionGeneration()
	c.advanceConnectionGeneration(current, reason)
	return true
}

func (c *AcquisitionCoordinator) isSourceAvailable() bool {
	c.sourceMu.RLock()
	defer c.sourceMu.RUnlock()
	return c.sourceAvailable
}

func (c *AcquisitionCoordinator) setSourceAvailable(available bool) {
	c.sourceMu.Lock()
	c.sourceAvailable = available
	c.sourceMu.Unlock()
	if available {
		c.signalPendingRebuilds()
	}
}

func (c *AcquisitionCoordinator) signalPendingRebuilds() {
	c.rebuildMu.Lock()
	hasPending := len(c.queued) > 0
	c.rebuildMu.Unlock()
	if !hasPending {
		return
	}
	select {
	case c.rebuildWake <- struct{}{}:
	default:
	}
}

func (c *AcquisitionCoordinator) recoverConnection(ctx context.Context) error {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.adapter.ConnectionState() != SourceConnected {
		return errSourceConnectionLost
	}

	if err := c.closeSubscriptions(ctx); err != nil {
		c.logWarn("Close old subscriptions during recovery failed", zap.Error(err))
	}
	generation := c.currentConnectionGeneration()
	discovery := c.adapter.Rediscover(ctx, generation)
	c.logDiscoveryIssues(discovery)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.adapter.ConnectionState() != SourceConnected {
		return errSourceConnectionLost
	}
	points := uniquePointIDs(discovery.Points)
	if len(points) == 0 {
		return discoveryFailure("full recovery discovery returned no points", discovery)
	}

	before := c.store.Snapshot()
	tokens := initialTokens(points, generation)
	if err := c.store.Initialize(points, tokens); err != nil {
		return fmt.Errorf("initialize recovered points: %w", err)
	}
	if len(discovery.Issues) == 0 {
		found := make(map[PointID]struct{}, len(points))
		for _, pointID := range points {
			found[pointID] = struct{}{}
		}
		for _, snapshot := range before {
			if _, exists := found[snapshot.PointID]; !exists {
				c.store.Remove(snapshot.PointID, tokenFromSnapshot(snapshot))
			}
		}
	}
	c.setDiscoveryPending(len(discovery.Issues) > 0)

	result := c.adapter.Subscribe(ctx, points, tokens, func(sample PointSample) {
		c.store.ApplySubscription(sample)
	})
	if result.Subscription != nil {
		c.trackSubscription(result.Subscription)
	}
	succeeded := make(map[PointID]struct{}, len(result.Succeeded))
	for _, pointID := range result.Succeeded {
		if _, exists := tokens[pointID]; exists && result.Subscription != nil {
			succeeded[pointID] = struct{}{}
			c.setOwner(pointID, result.Subscription)
		}
	}
	for _, failure := range result.Failed {
		if token, exists := tokens[failure.PointID]; exists {
			c.store.MarkPointFailed(failure.PointID, token, PointSubscribeFailed, failure.Reason)
		}
	}
	for _, pointID := range points {
		if _, ok := succeeded[pointID]; !ok {
			c.store.MarkPointFailed(pointID, tokens[pointID], PointSubscribeFailed, "recovery did not create a monitored item")
		}
	}
	c.readRecovery(ctx, points)
	if c.adapter.ConnectionState() != SourceConnected {
		return errSourceConnectionLost
	}
	for pointID := range succeeded {
		snapshot, ok := c.store.Get(pointID)
		if ok && snapshot.HasValue && snapshot.Validity == SampleValid && snapshot.Health == PointHealthy {
			return nil
		}
	}
	return errors.New("no recovered point has both a working subscription and a valid value")
}

func (c *AcquisitionCoordinator) readRecovery(ctx context.Context, points []PointID) {
	for _, batch := range pointBatches(points, c.config.readBatchSize) {
		versions, tokens := c.capture(batch)
		batchCtx, cancel := context.WithTimeout(ctx, c.config.readTimeout)
		result := c.adapter.Verify(batchCtx, batch, tokens)
		cancel()
		if result.RequestFailure != "" {
			c.logWarn("Recovery verification request failed", zap.String("reason", result.RequestFailure))
			continue
		}
		for _, verification := range result.Verifications {
			expectedVersion, ok := versions[verification.PointID]
			if !ok {
				continue
			}
			current, exists := c.store.Get(verification.PointID)
			if !exists || current.Version != expectedVersion {
				continue
			}
			if !current.HasValue && c.store.SeedInitialValue(verification, expectedVersion) {
				continue
			}
			c.store.ApplyVerification(verification, expectedVersion)
		}
		c.applyReadFailures(result.Failed, versions, tokens)
	}
}

func initialTokens(points []PointID, connectionGeneration uint64) map[PointID]GenerationToken {
	tokens := make(map[PointID]GenerationToken, len(points))
	for _, pointID := range points {
		tokens[pointID] = GenerationToken{Connection: connectionGeneration, Monitor: 1}
	}
	return tokens
}

func uniquePointIDs(points []PointID) []PointID {
	seen := make(map[PointID]struct{}, len(points))
	result := make([]PointID, 0, len(points))
	for _, pointID := range points {
		if pointID == "" {
			continue
		}
		if _, exists := seen[pointID]; exists {
			continue
		}
		seen[pointID] = struct{}{}
		result = append(result, pointID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func pointBatches(points []PointID, batchSize int) [][]PointID {
	result := make([][]PointID, 0, (len(points)+batchSize-1)/batchSize)
	for start := 0; start < len(points); start += batchSize {
		end := start + batchSize
		if end > len(points) {
			end = len(points)
		}
		result = append(result, points[start:end])
	}
	return result
}

func snapshotPointIDs(snapshots []PointSnapshot) []PointID {
	points := make([]PointID, len(snapshots))
	for index, snapshot := range snapshots {
		points[index] = snapshot.PointID
	}
	return points
}

func tokenFromSnapshot(snapshot PointSnapshot) GenerationToken {
	return GenerationToken{Connection: snapshot.ConnectionGeneration, Monitor: snapshot.MonitorGeneration}
}

func staleCanChangeHealth(health PointHealth) bool {
	switch health {
	case PointInitializing, PointHealthy, PointVerificationFailed, PointStale:
		return true
	default:
		return false
	}
}

func failureReason(failures []PointFailure, pointID PointID, fallback string) string {
	for _, failure := range failures {
		if failure.PointID == pointID && failure.Reason != "" {
			return failure.Reason
		}
	}
	return fallback
}
