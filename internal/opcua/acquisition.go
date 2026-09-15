package opcua

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	"go-opcua-connector/internal/collector"

	goopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

const monitoredItemsPerSubscription = 2000

type monitoredSubscription interface {
	monitor(ctx context.Context, timestamps ua.TimestampsToReturn, items ...*ua.MonitoredItemCreateRequest) (*ua.CreateMonitoredItemsResponse, error)
	unmonitor(ctx context.Context, monitoredItemIDs ...uint32) (*ua.DeleteMonitoredItemsResponse, error)
	cancel(ctx context.Context) error
}

type acquisitionBackend interface {
	subscribe(ctx context.Context, params *goopcua.SubscriptionParameters, notifications chan *goopcua.PublishNotificationData) (monitoredSubscription, error)
	read(ctx context.Context, request *ua.ReadRequest) (*ua.ReadResponse, error)
}

type liveAcquisitionBackend struct {
	client *goopcua.Client
}

func (b *liveAcquisitionBackend) subscribe(
	ctx context.Context,
	params *goopcua.SubscriptionParameters,
	notifications chan *goopcua.PublishNotificationData,
) (monitoredSubscription, error) {
	subscription, err := b.client.Subscribe(ctx, params, notifications)
	if err != nil {
		return nil, err
	}
	return &liveMonitoredSubscription{subscription: subscription}, nil
}

func (b *liveAcquisitionBackend) read(ctx context.Context, request *ua.ReadRequest) (*ua.ReadResponse, error) {
	return b.client.Read(ctx, request)
}

type liveMonitoredSubscription struct {
	subscription *goopcua.Subscription
}

func (s *liveMonitoredSubscription) monitor(
	ctx context.Context,
	timestamps ua.TimestampsToReturn,
	items ...*ua.MonitoredItemCreateRequest,
) (response *ua.CreateMonitoredItemsResponse, err error) {
	// gopcua v0.5.3 会在响应缺项时直接索引越界；Adapter 必须把该情况转换为失败。
	defer func() {
		if recovered := recover(); recovered != nil {
			response = nil
			err = fmt.Errorf("gopcua monitor response is incomplete: %v", recovered)
		}
	}()
	return s.subscription.Monitor(ctx, timestamps, items...)
}

func (s *liveMonitoredSubscription) cancel(ctx context.Context) error {
	return s.subscription.Cancel(ctx)
}

func (s *liveMonitoredSubscription) unmonitor(ctx context.Context, monitoredItemIDs ...uint32) (*ua.DeleteMonitoredItemsResponse, error) {
	return s.subscription.Unmonitor(ctx, monitoredItemIDs...)
}

type monitoredPoint struct {
	pointID    collector.PointID
	sourceRef  SourceRef
	generation collector.GenerationToken
}

type pointTarget struct {
	monitoredPoint
	nodeID *ua.NodeID
	index  int
}

type monitoredItemBinding struct {
	point           monitoredPoint
	monitoredItemID uint32
}

type monitoredPointRegistry struct {
	mu       sync.RWMutex
	byHandle map[uint32]monitoredPoint
}

func newMonitoredPointRegistry(bindings map[uint32]monitoredItemBinding) *monitoredPointRegistry {
	registry := &monitoredPointRegistry{byHandle: make(map[uint32]monitoredPoint, len(bindings))}
	for handle, binding := range bindings {
		registry.byHandle[handle] = binding.point
	}
	return registry
}

func (r *monitoredPointRegistry) get(handle uint32) (monitoredPoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	point, ok := r.byHandle[handle]
	return point, ok
}

func (r *monitoredPointRegistry) remove(handle uint32) {
	r.mu.Lock()
	delete(r.byHandle, handle)
	r.mu.Unlock()
}

type managedPointRegistration struct {
	subscription    monitoredSubscription
	registry        *monitoredPointRegistry
	handle          uint32
	monitoredItemID uint32
}

type managedPointSubscription struct {
	ctx           context.Context
	cancelContext context.CancelFunc
	mu            sync.Mutex
	closing       bool
	subscriptions []monitoredSubscription
	registrations map[collector.PointID]managedPointRegistration
	wg            sync.WaitGroup
	closeOnce     sync.Once
	done          chan struct{}
	closeErr      error
}

func newManagedPointSubscription(parent context.Context) *managedPointSubscription {
	ctx, cancel := context.WithCancel(parent)
	return &managedPointSubscription{
		ctx:           ctx,
		cancelContext: cancel,
		registrations: make(map[collector.PointID]managedPointRegistration),
		done:          make(chan struct{}),
	}
}

func (s *managedPointSubscription) add(
	subscription monitoredSubscription,
	notifications <-chan *goopcua.PublishNotificationData,
	bindings map[uint32]monitoredItemBinding,
	now func() time.Time,
	logger *zap.Logger,
	handler collector.SampleHandler,
) {
	registry := newMonitoredPointRegistry(bindings)
	s.mu.Lock()
	s.subscriptions = append(s.subscriptions, subscription)
	for handle, binding := range bindings {
		s.registrations[binding.point.pointID] = managedPointRegistration{
			subscription: subscription, registry: registry, handle: handle, monitoredItemID: binding.monitoredItemID,
		}
	}
	s.mu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		consumePointNotifications(s.ctx, notifications, registry, now, logger, handler)
	}()
}

func (s *managedPointSubscription) Remove(ctx context.Context, pointID collector.PointID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return errors.New("point subscription is closing")
	}
	registration, ok := s.registrations[pointID]
	if !ok {
		return nil
	}
	response, err := registration.subscription.unmonitor(ctx, registration.monitoredItemID)
	if err != nil {
		return err
	}
	if response == nil || len(response.Results) == 0 {
		return errors.New("delete monitored item response missing")
	}
	if response.ResponseHeader != nil && response.ResponseHeader.ServiceResult != ua.StatusOK {
		return fmt.Errorf("delete monitored item request rejected: %s", response.ResponseHeader.ServiceResult)
	}
	if response.Results[0] != ua.StatusOK {
		return fmt.Errorf("delete monitored item rejected: %s", response.Results[0])
	}
	// 协议侧确认删除后再提交本地删除，失败时保留登记供 Close 最终回收。
	delete(s.registrations, pointID)
	registration.registry.remove(registration.handle)
	return nil
}

func (s *managedPointSubscription) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		subscriptions := append([]monitoredSubscription(nil), s.subscriptions...)
		s.mu.Unlock()
		go func() {
			var closeErrors []error
			// 先取消协议订阅并继续消费在途通知，再停止本地转发。
			for _, subscription := range subscriptions {
				if err := subscription.cancel(ctx); err != nil {
					closeErrors = append(closeErrors, err)
				}
			}
			s.cancelContext()
			s.wg.Wait()
			s.closeErr = errors.Join(closeErrors...)
			close(s.done)
		}()
	})

	select {
	case <-s.done:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubscribePoints 创建新采集链路使用的逐点可诊断订阅，不接入旧 Collector。
func (c *Client) SubscribePoints(
	ctx context.Context,
	points []collector.PointID,
	tokenByPoint map[collector.PointID]collector.GenerationToken,
	handler collector.SampleHandler,
) collector.SubscriptionResult {
	targets, failures := c.resolvePointTargets(points, tokenByPoint)
	result := collector.SubscriptionResult{Failed: failures}
	if len(targets) == 0 {
		return result
	}
	if handler == nil {
		result.Failed = appendFailures(result.Failed, targets, "sample handler is nil")
		sortPointFailures(result.Failed)
		return result
	}

	backend, err := c.currentAcquisitionBackend()
	if err != nil {
		result.Failed = appendFailures(result.Failed, targets, "source is not connected")
		sortPointFailures(result.Failed)
		return result
	}
	handles, err := c.allocateClientHandles(len(targets))
	if err != nil {
		result.Failed = appendFailures(result.Failed, targets, "ClientHandle space exhausted")
		sortPointFailures(result.Failed)
		return result
	}
	for index := range targets {
		targets[index].index = index
	}

	managed := newManagedPointSubscription(ctx)
	for start := 0; start < len(targets); start += monitoredItemsPerSubscription {
		end := start + monitoredItemsPerSubscription
		if end > len(targets) {
			end = len(targets)
		}
		batch := targets[start:end]
		notifications := make(chan *goopcua.PublishNotificationData, 100)
		subscription, err := backend.subscribe(ctx, &goopcua.SubscriptionParameters{
			Interval: 100 * time.Millisecond,
		}, notifications)
		if err != nil {
			c.logAdapterError("create subscription failed", err)
			result.Failed = appendFailures(result.Failed, batch, "subscription creation failed")
			continue
		}

		requests := make([]*ua.MonitoredItemCreateRequest, len(batch))
		for index, target := range batch {
			request := goopcua.NewMonitoredItemCreateRequestWithDefaults(
				target.nodeID,
				ua.AttributeIDValue,
				handles[target.index],
			)
			request.RequestedParameters.QueueSize = 1
			request.RequestedParameters.DiscardOldest = true
			request.RequestedParameters.SamplingInterval = 0
			requests[index] = request
		}

		monitorCtx, cancel := context.WithTimeout(ctx, c.adapterRequestTimeout())
		response, monitorErr := subscription.monitor(monitorCtx, ua.TimestampsToReturnBoth, requests...)
		cancel()
		if monitorErr != nil {
			c.logAdapterError("create monitored items failed", monitorErr)
			result.Failed = appendFailures(result.Failed, batch, "monitored item creation failed")
			_ = subscription.cancel(ctx)
			continue
		}

		succeeded, failed, handleMap := evaluateMonitorResponse(batch, handles[start:end], response, c.logger)
		result.Succeeded = append(result.Succeeded, succeeded...)
		result.Failed = append(result.Failed, failed...)
		if len(handleMap) == 0 {
			_ = subscription.cancel(ctx)
			continue
		}
		managed.add(subscription, notifications, handleMap, c.now, c.logger, handler)
	}

	sort.Slice(result.Succeeded, func(i, j int) bool { return result.Succeeded[i] < result.Succeeded[j] })
	sortPointFailures(result.Failed)
	if len(managed.subscriptions) > 0 {
		result.Subscription = managed
	} else {
		managed.cancelContext()
	}
	return result
}

func evaluateMonitorResponse(
	targets []pointTarget,
	handles []uint32,
	response *ua.CreateMonitoredItemsResponse,
	logger *zap.Logger,
) ([]collector.PointID, []collector.PointFailure, map[uint32]monitoredItemBinding) {
	succeeded := make([]collector.PointID, 0, len(targets))
	failed := make([]collector.PointFailure, 0)
	handleMap := make(map[uint32]monitoredItemBinding, len(targets))
	if response != nil && response.ResponseHeader != nil && response.ResponseHeader.ServiceResult != ua.StatusOK {
		if logger != nil {
			logger.Warn("CreateMonitoredItems request rejected",
				zap.String("status_code", fmt.Sprint(response.ResponseHeader.ServiceResult)))
		}
		return succeeded, appendFailures(failed, targets, "monitored item request rejected"), handleMap
	}
	if response != nil && len(response.Results) != len(targets) && logger != nil {
		logger.Warn("CreateMonitoredItems result count mismatch",
			zap.Int("requested", len(targets)),
			zap.Int("received", len(response.Results)))
	}
	for index, target := range targets {
		if response == nil || index >= len(response.Results) || response.Results[index] == nil {
			failed = append(failed, collector.PointFailure{PointID: target.pointID, Reason: "monitored item response missing"})
			continue
		}
		monitorResult := response.Results[index]
		if monitorResult.StatusCode != ua.StatusOK {
			if logger != nil {
				logger.Warn("Monitored item rejected",
					zap.String("point_id", string(target.pointID)),
					zap.String("source_ref", target.sourceRef.String()),
					zap.String("status_code", fmt.Sprint(monitorResult.StatusCode)))
			}
			failed = append(failed, collector.PointFailure{PointID: target.pointID, Reason: "monitored item rejected"})
			continue
		}
		succeeded = append(succeeded, target.pointID)
		handleMap[handles[index]] = monitoredItemBinding{point: target.monitoredPoint, monitoredItemID: monitorResult.MonitoredItemID}
	}
	return succeeded, failed, handleMap
}

func consumePointNotifications(
	ctx context.Context,
	notifications <-chan *goopcua.PublishNotificationData,
	handles *monitoredPointRegistry,
	now func() time.Time,
	logger *zap.Logger,
	handler collector.SampleHandler,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case notification, ok := <-notifications:
			if !ok {
				return
			}
			if notification == nil {
				continue
			}
			if notification.Error != nil {
				if logger != nil {
					logger.Error("OPC UA publish failed", zap.Error(notification.Error))
				}
				continue
			}
			data, ok := notification.Value.(*ua.DataChangeNotification)
			if !ok || data == nil {
				continue
			}
			for _, item := range data.MonitoredItems {
				if item == nil {
					continue
				}
				point, exists := handles.get(item.ClientHandle)
				if !exists {
					if logger != nil {
						logger.Warn("Unknown OPC UA ClientHandle", zap.Uint32("client_handle", item.ClientHandle))
					}
					continue
				}
				receivedAt := now()
				handler(normalizePointSample(point, item.Value, receivedAt))
			}
		}
	}
}

func normalizePointSample(point monitoredPoint, value *ua.DataValue, receivedAt time.Time) collector.PointSample {
	sample := collector.PointSample{
		PointID:       point.pointID,
		Validity:      collector.SampleInvalid,
		ObservedAt:    receivedAt,
		ReceivedAt:    receivedAt,
		FailureReason: "subscription sample has no data value",
		Generation:    point.generation,
	}
	if value == nil {
		return sample
	}
	sample.ObservedAt = observedAt(value, receivedAt)
	if value.Value != nil {
		sample.Value = cloneAdapterValue(value.Value.Value())
		sample.HasValue = true
	}
	if !statusIsGood(value.Status) {
		sample.FailureReason = "subscription sample status is not good"
		return sample
	}
	if !sample.HasValue {
		sample.FailureReason = "subscription sample has no value"
		return sample
	}
	sample.Validity = collector.SampleValid
	sample.FailureReason = ""
	return sample
}

// ReadInitial 对 PointID 执行一次批量读取，并转换为 StateStore 可消费的 Verification。
// 分批和每批超时由 Task 4 的采集协调器负责。
func (c *Client) ReadInitial(
	ctx context.Context,
	points []collector.PointID,
	tokenByPoint map[collector.PointID]collector.GenerationToken,
) collector.VerificationResult {
	targets, failures := c.resolvePointTargets(points, tokenByPoint)
	result := collector.VerificationResult{Failed: failures}
	if len(targets) == 0 {
		return result
	}
	backend, err := c.currentAcquisitionBackend()
	if err != nil {
		result.RequestFailure = "source is not connected"
		return result
	}

	nodes := make([]*ua.ReadValueID, len(targets))
	for index, target := range targets {
		nodes[index] = &ua.ReadValueID{NodeID: target.nodeID, AttributeID: ua.AttributeIDValue}
	}
	response, readErr := backend.read(ctx, &ua.ReadRequest{
		NodesToRead:        nodes,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	})
	if readErr != nil {
		c.logAdapterError("initial read failed", readErr)
		result.RequestFailure = "read request failed"
		return result
	}
	if response != nil && response.ResponseHeader != nil && response.ResponseHeader.ServiceResult != ua.StatusOK {
		if c.logger != nil {
			c.logger.Warn("Initial read request rejected",
				zap.String("status_code", fmt.Sprint(response.ResponseHeader.ServiceResult)))
		}
		result.RequestFailure = "read request rejected"
		return result
	}

	for index, target := range targets {
		if response == nil || index >= len(response.Results) || response.Results[index] == nil {
			result.Failed = append(result.Failed, collector.PointFailure{PointID: target.pointID, Reason: "read response missing"})
			continue
		}
		checkedAt := c.now()
		result.Verifications = append(result.Verifications, normalizeVerification(target.monitoredPoint, response.Results[index], checkedAt))
	}
	sort.Slice(result.Verifications, func(i, j int) bool {
		return result.Verifications[i].PointID < result.Verifications[j].PointID
	})
	sortPointFailures(result.Failed)
	return result
}

func normalizeVerification(point monitoredPoint, value *ua.DataValue, checkedAt time.Time) collector.Verification {
	verification := collector.Verification{
		PointID:       point.pointID,
		Validity:      collector.SampleInvalid,
		ObservedAt:    observedAt(value, checkedAt),
		CheckedAt:     checkedAt,
		FailureReason: "read result has no value",
		Generation:    point.generation,
	}
	if value.Value != nil {
		verification.Value = cloneAdapterValue(value.Value.Value())
		verification.HasValue = true
	}
	if !statusIsGood(value.Status) {
		verification.FailureReason = "read result status is not good"
		return verification
	}
	if !verification.HasValue {
		return verification
	}
	verification.Validity = collector.SampleValid
	verification.FailureReason = ""
	return verification
}

func observedAt(value *ua.DataValue, fallback time.Time) time.Time {
	if value == nil {
		return fallback
	}
	if !value.SourceTimestamp.IsZero() {
		return value.SourceTimestamp
	}
	if !value.ServerTimestamp.IsZero() {
		return value.ServerTimestamp
	}
	return fallback
}

func statusIsGood(status ua.StatusCode) bool {
	return status&0xC0000000 == 0
}

func (c *Client) resolvePointTargets(
	points []collector.PointID,
	tokenByPoint map[collector.PointID]collector.GenerationToken,
) ([]pointTarget, []collector.PointFailure) {
	pointIDs := uniqueSortedPointIDs(points)
	targets := make([]pointTarget, 0, len(pointIDs))
	failures := make([]collector.PointFailure, 0)
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	for _, pointID := range pointIDs {
		token, exists := tokenByPoint[pointID]
		if !exists || token.Connection == 0 || token.Monitor == 0 {
			failures = append(failures, collector.PointFailure{PointID: pointID, Reason: "generation token is missing or invalid"})
			continue
		}
		sourceRef, exists := c.sourceByPointID[string(pointID)]
		if !exists {
			failures = append(failures, collector.PointFailure{PointID: pointID, Reason: "point identity not found"})
			continue
		}
		nodeID, err := sourceRef.node()
		if err != nil {
			failures = append(failures, collector.PointFailure{PointID: pointID, Reason: "point source reference is invalid"})
			continue
		}
		targets = append(targets, pointTarget{
			monitoredPoint: monitoredPoint{pointID: pointID, sourceRef: sourceRef, generation: token},
			nodeID:         nodeID,
		})
	}
	return targets, failures
}

func uniqueSortedPointIDs(points []collector.PointID) []collector.PointID {
	seen := make(map[collector.PointID]struct{}, len(points))
	result := make([]collector.PointID, 0, len(points))
	for _, pointID := range points {
		if _, exists := seen[pointID]; exists {
			continue
		}
		seen[pointID] = struct{}{}
		result = append(result, pointID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (c *Client) currentAcquisitionBackend() (acquisitionBackend, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.adapterBackend != nil {
		return c.adapterBackend, nil
	}
	if c.client == nil {
		return nil, errors.New("client not connected")
	}
	return &liveAcquisitionBackend{client: c.client}, nil
}

func (c *Client) allocateClientHandles(count int) ([]uint32, error) {
	if count < 0 {
		return nil, errors.New("invalid ClientHandle count")
	}
	c.handleMu.Lock()
	defer c.handleMu.Unlock()
	if c.nextClientHandle == 0 {
		c.nextClientHandle = 1
	}
	if uint64(count) > uint64(math.MaxUint32)-c.nextClientHandle+1 {
		return nil, errors.New("ClientHandle space exhausted")
	}
	handles := make([]uint32, count)
	for index := range handles {
		handles[index] = uint32(c.nextClientHandle)
		c.nextClientHandle++
	}
	return handles, nil
}

func (c *Client) resetClientHandles() {
	c.handleMu.Lock()
	c.nextClientHandle = 1
	c.handleMu.Unlock()
}

func (c *Client) adapterRequestTimeout() time.Duration {
	if c.config != nil && c.config.RequestTimeout > 0 {
		return time.Duration(c.config.RequestTimeout) * time.Second
	}
	return 30 * time.Second
}

func (c *Client) now() time.Time {
	if c.adapterNow != nil {
		return c.adapterNow()
	}
	return time.Now()
}

func (c *Client) logAdapterError(message string, err error) {
	if c.logger != nil {
		c.logger.Error(message, zap.Error(err))
	}
}

func appendFailures(failures []collector.PointFailure, targets []pointTarget, reason string) []collector.PointFailure {
	for _, target := range targets {
		failures = append(failures, collector.PointFailure{PointID: target.pointID, Reason: reason})
	}
	return failures
}

func sortPointFailures(failures []collector.PointFailure) {
	sort.Slice(failures, func(i, j int) bool {
		if failures[i].PointID == failures[j].PointID {
			return failures[i].Reason < failures[j].Reason
		}
		return failures[i].PointID < failures[j].PointID
	})
}

func cloneAdapterValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneAdapterReflectValue(reflect.ValueOf(value)).Interface()
}

func cloneAdapterReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(cloneAdapterReflectValue(value.Elem()))
		return result
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloneAdapterReflectValue(value.Elem()))
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			result.Index(index).Set(cloneAdapterReflectValue(value.Index(index)))
		}
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			result.SetMapIndex(iterator.Key(), cloneAdapterReflectValue(iterator.Value()))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			result.Index(index).Set(cloneAdapterReflectValue(value.Index(index)))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for index := 0; index < value.NumField(); index++ {
			if result.Field(index).CanSet() && value.Field(index).CanInterface() {
				result.Field(index).Set(cloneAdapterReflectValue(value.Field(index)))
			}
		}
		return result
	default:
		return value
	}
}
