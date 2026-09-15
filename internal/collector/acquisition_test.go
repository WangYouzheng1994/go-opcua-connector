package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go-opcua-connector/internal/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestAcquisitionStartReportsDiscoveryFailureDetails(t *testing.T) {
	t.Parallel()

	issues := []DiscoveryIssue{
		{Source: "configured.point", Reason: "cannot resolve source path"},
		{Reason: "discovery request failed"},
	}
	adapter := &fakeSourceAdapter{discoverFn: func(uint64) DiscoveryResult {
		return DiscoveryResult{Issues: issues}
	}}
	coordinator := newTestCoordinator(t, adapter, NewStateStore())
	err := coordinator.Start(context.Background())
	if err == nil {
		t.Fatal("expected discovery failure")
	}
	for _, detail := range []string{"source discovery returned no points", "configured.point", "cannot resolve source path", "discovery request failed"} {
		if !strings.Contains(err.Error(), detail) {
			t.Fatalf("startup error lost %q: %v", detail, err)
		}
	}
}

func TestAcquisitionStartMergesSubscriptionBeforeInitialRead(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	adapter := &fakeSourceAdapter{points: []PointID{"point.b", "point.a", "point.a"}}
	adapter.subscribeFn = func(_ []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		handler(PointSample{PointID: "point.a", Value: "subscription", HasValue: true, Validity: SampleValid, ObservedAt: now, ReceivedAt: now, Generation: tokens["point.a"]})
		return SubscriptionResult{Succeeded: []PointID{"point.a", "point.b"}, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func(points []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		pointID := points[0]
		value := any("initial-b")
		if pointID == "point.a" {
			value = "old-read"
		}
		return VerificationResult{Verifications: []Verification{{
			PointID: pointID, Value: value, HasValue: true, Validity: SampleValid,
			ObservedAt: now.Add(-time.Second), CheckedAt: now, Generation: tokens[pointID],
		}}}
	}
	coordinator := newTestCoordinator(t, adapter, newStateStore(func() time.Time { return now }))
	coordinator.config.readBatchSize = 1
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Stop(context.Background()) })

	pointA, _ := coordinator.store.Get("point.a")
	pointB, _ := coordinator.store.Get("point.b")
	if pointA.Value != "subscription" || pointA.Health != PointHealthy {
		t.Fatalf("initial read overwrote subscription: %#v", pointA)
	}
	if pointB.Value != "initial-b" || pointB.Health != PointHealthy {
		t.Fatalf("initial read did not seed point.b: %#v", pointB)
	}
	if !reflect.DeepEqual(adapter.verifiedBatches(), [][]PointID{{"point.a"}, {"point.b"}}) {
		t.Fatalf("read batches are not stable: %#v", adapter.verifiedBatches())
	}
	if !reflect.DeepEqual(adapter.discoveredGenerations(), []uint64{1}) {
		t.Fatalf("initial discovery generations = %#v", adapter.discoveredGenerations())
	}
}

func TestAcquisitionStartFailureClosesOnlyOwnedSubscriptions(t *testing.T) {
	t.Parallel()

	adapter := &fakeSourceAdapter{points: []PointID{"point.a"}}
	adapter.subscribeFn = func(_ []PointID, _ map[PointID]GenerationToken, _ SampleHandler) SubscriptionResult {
		return SubscriptionResult{Succeeded: []PointID{"point.a"}, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func([]PointID, map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{RequestFailure: "transport unavailable"}
	}
	coordinator := newTestCoordinator(t, adapter, NewStateStore())
	if err := coordinator.Start(context.Background()); err == nil {
		t.Fatal("expected startup failure")
	}
	if adapter.closeCount() != 0 || adapter.subscription.closeCount() != 1 {
		t.Fatalf("startup cleanup: adapter=%d subscription=%d", adapter.closeCount(), adapter.subscription.closeCount())
	}
}

func TestVerificationRequestFailureDoesNotPollutePointState(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 7))
	before, _ := store.Get(pointID)
	adapter := &fakeSourceAdapter{verifyFn: func([]PointID, map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{RequestFailure: "request timeout"}
	}}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.runVerificationRound(context.Background())
	after, _ := store.Get(pointID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("whole request failure changed point state:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestMismatchRecheckCancelsRebuildWhenSubscriptionCatchesUp(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 1))
	var calls int
	adapter := &fakeSourceAdapter{}
	adapter.verifyFn = func(_ []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		calls++
		if calls == 2 {
			store.ApplySubscription(validSample(pointID, tokens[pointID], 2))
		}
		return VerificationResult{Verifications: []Verification{{
			PointID: pointID, Value: 2, HasValue: true, Validity: SampleValid,
			CheckedAt: time.Now(), Generation: tokens[pointID], FailureReason: "subscription value differs from verification",
		}}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.config.mismatchRecheckDelay = time.Millisecond
	coordinator.runVerificationRound(context.Background())
	after, _ := store.Get(pointID)
	if after.Health != PointHealthy || after.Value != 2 {
		t.Fatalf("subscription catch-up was not retained: %#v", after)
	}
	coordinator.rebuildMu.Lock()
	queued := len(coordinator.queued)
	coordinator.rebuildMu.Unlock()
	if queued != 0 {
		t.Fatalf("point was queued after subscription catch-up")
	}
}

func TestConfirmedMismatchQueuesOnlyThatPointForRebuild(t *testing.T) {
	t.Parallel()

	store := newStateStore(time.Now)
	points := []PointID{"point.a", "point.b"}
	tokens := initialTokens(points, 1)
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	store.ApplySubscription(validSample("point.a", tokens["point.a"], 1))
	store.ApplySubscription(validSample("point.b", tokens["point.b"], 2))
	adapter := &fakeSourceAdapter{verifyFn: func(requested []PointID, currentTokens map[PointID]GenerationToken) VerificationResult {
		results := make([]Verification, 0, len(requested))
		for _, pointID := range requested {
			value := any(2)
			if pointID == "point.a" {
				value = 99
			}
			results = append(results, Verification{PointID: pointID, Value: value, HasValue: true, Validity: SampleValid, CheckedAt: time.Now(), Generation: currentTokens[pointID]})
		}
		return VerificationResult{Verifications: results}
	}}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.ctx = context.Background()
	coordinator.config.mismatchRecheckDelay = time.Millisecond
	coordinator.runVerificationRound(coordinator.ctx)
	select {
	case <-coordinator.rebuildWake:
	default:
		t.Fatal("confirmed mismatch was not queued")
	}
	coordinator.rebuildMu.Lock()
	_, queued := coordinator.queued["point.a"]
	queuedCount := len(coordinator.queued)
	coordinator.rebuildMu.Unlock()
	if !queued || queuedCount != 1 {
		t.Fatalf("point.a queued=%v, queued count=%d", queued, queuedCount)
	}
	pointB, _ := store.Get("point.b")
	if pointB.Health != PointHealthy {
		t.Fatalf("matching point was affected: %#v", pointB)
	}
}

func TestRetryLoopDoesNotDeadlockWhenFailedPointsExceedQueueCapacity(t *testing.T) {
	const pointCount = 300

	store := newStateStore(time.Now)
	points := make([]PointID, pointCount)
	tokens := make(map[PointID]GenerationToken, pointCount)
	for index := range points {
		pointID := PointID(fmt.Sprintf("point.%03d", index))
		points[index] = pointID
		tokens[pointID] = GenerationToken{Connection: 1, Monitor: 1}
	}
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	for _, pointID := range points {
		store.MarkPointFailed(pointID, tokens[pointID], PointSubscribeFailed, "initial subscribe failed")
	}

	processed := make(chan PointID, pointCount)
	adapter := &fakeSourceAdapter{}
	adapter.subscribeFn = func(requested []PointID, currentTokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		pointID := requested[0]
		handler(validSample(pointID, currentTokens[pointID], string(pointID)))
		processed <- pointID
		return SubscriptionResult{Succeeded: requested, Subscription: &fakePointSubscription{}}
	}
	adapter.verifyFn = func(requested []PointID, currentTokens map[PointID]GenerationToken) VerificationResult {
		pointID := requested[0]
		return VerificationResult{Verifications: []Verification{{
			PointID: pointID, Value: string(pointID), HasValue: true, Validity: SampleValid,
			CheckedAt: time.Now(), Generation: currentTokens[pointID],
		}}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.config.retryInterval = time.Millisecond
	coordinator.ctx, coordinator.cancel = context.WithCancel(context.Background())
	coordinator.started = true
	coordinator.wg.Add(1)
	go coordinator.retryLoop()

	seen := make(map[PointID]struct{}, pointCount)
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for len(seen) < pointCount {
		select {
		case pointID := <-processed:
			seen[pointID] = struct{}{}
		case <-timeout.C:
			t.Fatalf("retry loop processed %d/%d points before timeout", len(seen), pointCount)
		}
	}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		coordinator.rebuildMu.Lock()
		pending := len(coordinator.queued)
		coordinator.rebuildMu.Unlock()
		if pending == 0 {
			break
		}
		select {
		case <-poll.C:
		case <-timeout.C:
			t.Fatalf("retry loop left %d pending points before timeout", pending)
		}
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	coordinator.rebuildMu.Lock()
	pending := len(coordinator.queued)
	coordinator.rebuildMu.Unlock()
	if pending != 0 {
		t.Fatalf("pending rebuilds after drain = %d", pending)
	}
}

func TestEnqueueRebuildDeduplicatesAndHonorsCancellation(t *testing.T) {
	t.Parallel()

	coordinator := newTestCoordinator(t, &fakeSourceAdapter{}, NewStateStore())
	coordinator.ctx, coordinator.cancel = context.WithCancel(context.Background())
	coordinator.enqueueRebuild("point.a")
	coordinator.enqueueRebuild("point.a")
	coordinator.rebuildMu.Lock()
	pending := len(coordinator.queued)
	coordinator.rebuildMu.Unlock()
	if pending != 1 || len(coordinator.rebuildWake) != 1 {
		t.Fatalf("pending=%d, wake signals=%d", pending, len(coordinator.rebuildWake))
	}
	coordinator.cancel()
	coordinator.enqueueRebuild("point.b")
	coordinator.rebuildMu.Lock()
	_, addedAfterCancel := coordinator.queued["point.b"]
	coordinator.rebuildMu.Unlock()
	if addedAfterCancel {
		t.Fatal("enqueue added a rebuild after cancellation")
	}
}

func TestStopExitsWithPendingRebuilds(t *testing.T) {
	t.Parallel()

	coordinator := newTestCoordinator(t, &fakeSourceAdapter{}, NewStateStore())
	coordinator.ctx, coordinator.cancel = context.WithCancel(context.Background())
	coordinator.started = true
	for index := 0; index < 300; index++ {
		coordinator.enqueueRebuild(PointID(fmt.Sprintf("pending.%03d", index)))
	}
	coordinator.wg.Add(1)
	go coordinator.retryLoop()
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() with pending rebuilds error = %v", err)
	}
	coordinator.enqueueRebuild("after-stop")
	coordinator.rebuildMu.Lock()
	_, addedAfterStop := coordinator.queued["after-stop"]
	coordinator.rebuildMu.Unlock()
	if addedAfterStop {
		t.Fatal("enqueue added a rebuild after Stop")
	}
}

func TestRebuildAdvancesGenerationBeforeRemovingOldMonitor(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 1))
	store.MarkPointFailed(pointID, token, PointSubscribeFailed, "monitor failed")
	oldSubscription := &fakePointSubscription{}
	oldSubscription.removeFn = func(PointID) {
		snapshot, _ := store.Get(pointID)
		if snapshot.MonitorGeneration != 2 {
			t.Errorf("old monitor removed before generation advanced: %#v", snapshot)
		}
	}
	adapter := &fakeSourceAdapter{}
	adapter.subscribeFn = func(_ []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		newToken := tokens[pointID]
		if newToken.Monitor != 2 {
			t.Fatalf("rebuild token = %#v", newToken)
		}
		handler(validSample(pointID, newToken, 99))
		return SubscriptionResult{Succeeded: []PointID{pointID}, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func(_ []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{Verifications: []Verification{{PointID: pointID, Value: 99, HasValue: true, Validity: SampleValid, CheckedAt: time.Now(), Generation: tokens[pointID]}}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.setOwner(pointID, oldSubscription)
	coordinator.rebuildPoint(context.Background(), pointID)
	after, _ := store.Get(pointID)
	if after.MonitorGeneration != 2 || after.Health != PointHealthy || after.Value != 99 {
		t.Fatalf("unexpected rebuilt state: %#v", after)
	}
	if !reflect.DeepEqual(oldSubscription.removedPoints(), []PointID{pointID}) {
		t.Fatalf("removed points = %#v", oldSubscription.removedPoints())
	}
	if store.ApplySubscription(validSample(pointID, token, "late-old")) {
		t.Fatal("old monitor generation remained valid")
	}
}

func TestStaleCheckUsesVersionAndPreservesSpecificFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	store := newStateStore(func() time.Time { return now })
	points := []PointID{"healthy", "invalid", "never"}
	tokens := initialTokens(points, 1)
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	store.ApplySubscription(PointSample{PointID: "healthy", Value: 1, HasValue: true, Validity: SampleValid, ObservedAt: now, ReceivedAt: now, Generation: tokens["healthy"]})
	store.ApplySubscription(PointSample{PointID: "invalid", Validity: SampleInvalid, ObservedAt: now, ReceivedAt: now, Generation: tokens["invalid"], FailureReason: "bad quality"})
	coordinator := newTestCoordinator(t, &fakeSourceAdapter{}, store)
	coordinator.config.staleThreshold = time.Second
	coordinator.markStale(now.Add(2 * time.Second))
	healthy, _ := store.Get("healthy")
	invalid, _ := store.Get("invalid")
	never, _ := store.Get("never")
	if healthy.Health != PointStale || healthy.Value != 1 {
		t.Fatalf("healthy point was not marked stale correctly: %#v", healthy)
	}
	if invalid.Health != PointSampleInvalid {
		t.Fatalf("stale check overwrote specific failure: %#v", invalid)
	}
	if never.Health != PointStale || never.HasValue {
		t.Fatalf("never-valid point did not use initialization time: %#v", never)
	}
}

func TestRetryDiscoveryAddsOnlyNewPoints(t *testing.T) {
	t.Parallel()

	store, pointA, tokenA := initializedStore(t)
	store.ApplySubscription(validSample(pointA, tokenA, 1))
	adapter := &fakeSourceAdapter{}
	adapter.discoverFn = func(uint64) DiscoveryResult {
		return DiscoveryResult{Points: []PointID{pointA, "point.new"}}
	}
	adapter.subscribeFn = func(points []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		if !reflect.DeepEqual(points, []PointID{"point.new"}) {
			t.Fatalf("retry subscribed unexpected points: %#v", points)
		}
		handler(validSample("point.new", tokens["point.new"], 2))
		return SubscriptionResult{Succeeded: points, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func(points []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{Verifications: []Verification{{PointID: points[0], Value: 2, HasValue: true, Validity: SampleValid, CheckedAt: time.Now(), Generation: tokens[points[0]]}}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.setDiscoveryPending(true)
	coordinator.retryDiscovery(context.Background())
	added, ok := store.Get("point.new")
	if !ok || added.Health != PointHealthy || added.Value != 2 {
		t.Fatalf("new discovery was not acquired: %#v, exists=%v", added, ok)
	}
	if coordinator.isDiscoveryPending() {
		t.Fatal("resolved discovery issue remained pending")
	}
	if !reflect.DeepEqual(adapter.discoveredGenerations(), []uint64{1}) {
		t.Fatalf("retry discovery generations = %#v", adapter.discoveredGenerations())
	}
}

func TestConnectionGenerationAdvanceDefinesNextDiscoveryGeneration(t *testing.T) {
	t.Parallel()

	store, pointID, oldToken := initializedStore(t)
	store.ApplySubscription(validSample(pointID, oldToken, 1))
	adapter := &fakeSourceAdapter{discoverFn: func(uint64) DiscoveryResult { return DiscoveryResult{} }}
	coordinator := newTestCoordinator(t, adapter, store)
	next, ok := coordinator.advanceConnectionGeneration(1, "connection changed")
	if !ok || next != 2 || coordinator.currentConnectionGeneration() != 2 {
		t.Fatalf("connection generation advance = %d, %v", next, ok)
	}
	coordinator.retryDiscovery(context.Background())
	if !reflect.DeepEqual(adapter.discoveredGenerations(), []uint64{2}) {
		t.Fatalf("discovery generations = %#v", adapter.discoveredGenerations())
	}
	snapshot, _ := store.Get(pointID)
	if snapshot.ConnectionGeneration != 2 || snapshot.MonitorGeneration != 0 {
		t.Fatalf("connection and monitor generations were mixed: %#v", snapshot)
	}
	if store.ApplySubscription(validSample(pointID, oldToken, "old sample")) {
		t.Fatal("old connection sample was accepted")
	}
	if store.ApplyVerification(Verification{
		PointID: pointID, Value: 1, HasValue: true, Validity: SampleValid,
		CheckedAt: time.Now(), Generation: oldToken,
	}, snapshot.Version) {
		t.Fatal("old connection verification was accepted")
	}
}

func TestRebuildRemoveFailureKeepsOldSubscriptionForFinalClose(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 1))
	store.MarkPointFailed(pointID, token, PointSubscribeFailed, "monitor failed")
	oldSubscription := &fakePointSubscription{removeErr: errors.New("unmonitor failed")}
	adapter := &fakeSourceAdapter{}
	adapter.subscribeFn = func(_ []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		handler(validSample(pointID, tokens[pointID], 2))
		return SubscriptionResult{Succeeded: []PointID{pointID}, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func(_ []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{Verifications: []Verification{{
			PointID: pointID, Value: 2, HasValue: true, Validity: SampleValid,
			CheckedAt: time.Now(), Generation: tokens[pointID],
		}}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.trackSubscription(oldSubscription)
	coordinator.setOwner(pointID, oldSubscription)
	coordinator.rebuildPoint(context.Background(), pointID)
	if err := coordinator.closeSubscriptions(context.Background()); err != nil {
		t.Fatalf("closeSubscriptions() error = %v", err)
	}
	if oldSubscription.closeCount() != 1 {
		t.Fatalf("old subscription close count = %d", oldSubscription.closeCount())
	}
	after, _ := store.Get(pointID)
	if after.MonitorGeneration != 2 || after.Health != PointHealthy || after.Value != 2 {
		t.Fatalf("new generation did not recover after remove failure: %#v", after)
	}
	if store.ApplySubscription(validSample(pointID, token, "late old sample")) {
		t.Fatal("old monitor generation changed state after remove failure")
	}
}

func TestStopIsIdempotentAndDoesNotCloseSharedAdapter(t *testing.T) {
	t.Parallel()

	now := time.Now()
	adapter := &fakeSourceAdapter{points: []PointID{"point.a"}}
	adapter.subscribeFn = func(_ []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		handler(PointSample{PointID: "point.a", Value: 1, HasValue: true, Validity: SampleValid, ObservedAt: now, ReceivedAt: now, Generation: tokens["point.a"]})
		return SubscriptionResult{Succeeded: []PointID{"point.a"}, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func(_ []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{Verifications: []Verification{{PointID: "point.a", Value: 1, HasValue: true, Validity: SampleValid, CheckedAt: now, Generation: tokens["point.a"]}}}
	}
	coordinator := newTestCoordinator(t, adapter, NewStateStore())
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	if adapter.closeCount() != 0 || adapter.subscription.closeCount() != 1 {
		t.Fatalf("normal stop cleanup: adapter=%d subscription=%d", adapter.closeCount(), adapter.subscription.closeCount())
	}
}

func TestStopDeadlineBoundsWaitingForOperationSerialization(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 1))
	operationEntered := make(chan struct{})
	releaseOperation := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOperation) }) })
	adapter := &fakeSourceAdapter{verifyFn: func([]PointID, map[PointID]GenerationToken) VerificationResult {
		close(operationEntered)
		<-releaseOperation // 模拟底层协议调用未及时响应 Context cancellation。
		return VerificationResult{RequestFailure: "released after stop deadline"}
	}}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.ctx, coordinator.cancel = context.WithCancel(context.Background())
	coordinator.started = true
	subscription := &fakePointSubscription{}
	coordinator.trackSubscription(subscription)
	operationDone := make(chan struct{})
	coordinator.wg.Add(1)
	go func() {
		defer coordinator.wg.Done()
		defer close(operationDone)
		coordinator.runVerificationRound(coordinator.ctx)
	}()
	select {
	case <-operationEntered:
	case <-time.After(time.Second):
		t.Fatal("serialized operation did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	startedAt := time.Now()
	err := coordinator.Stop(stopCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("Stop() waited past its deadline: %v", elapsed)
	}
	if subscription.closeCount() != 0 {
		t.Fatalf("subscription closed before serialized operation released: %d", subscription.closeCount())
	}

	releaseOnce.Do(func() { close(releaseOperation) })
	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("serialized operation did not finish after release")
	}
	finalCtx, finalCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalCancel()
	if err := coordinator.Stop(finalCtx); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	if err := coordinator.Stop(finalCtx); err != nil {
		t.Fatalf("third Stop() error = %v", err)
	}
	if subscription.closeCount() != 1 {
		t.Fatalf("subscription close count = %d, want 1", subscription.closeCount())
	}
}

func TestAcquisitionStopContextAddsDefaultDeadline(t *testing.T) {
	t.Parallel()

	startedAt := time.Now()
	ctx, cancel := acquisitionStopContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("default stop context has no deadline")
	}
	remaining := deadline.Sub(startedAt)
	if remaining < 9*time.Second || remaining > 11*time.Second {
		t.Fatalf("default stop timeout = %v", remaining)
	}
}

func TestConnectionStateDisconnectAdvancesGenerationOnlyOnce(t *testing.T) {
	t.Parallel()

	store, pointID, oldToken := initializedStore(t)
	store.ApplySubscription(validSample(pointID, oldToken, "last-good"))
	coordinator := newTestCoordinator(t, &fakeSourceAdapter{}, store)
	var logs bytes.Buffer
	coordinator.logger = jsonTestLogger(&logs)
	nextRecovery := time.Time{}
	backoffIndex := 0
	coordinator.handleConnectionState(context.Background(), SourceDisconnected, time.Now(), &nextRecovery, &backoffIndex)
	coordinator.handleConnectionState(context.Background(), SourceReconnecting, time.Now(), &nextRecovery, &backoffIndex)
	if count := strings.Count(logs.String(), `"msg":"Source connection unavailable"`); count != 1 {
		t.Fatalf("connection loss log count = %d, want 1: %s", count, logs.String())
	}

	snapshot, _ := store.Get(pointID)
	if snapshot.ConnectionGeneration != 2 || snapshot.MonitorGeneration != 0 || snapshot.Health != PointSourceDisconnected {
		t.Fatalf("unexpected disconnected state: %#v", snapshot)
	}
	if snapshot.Value != "last-good" || !snapshot.HasValue {
		t.Fatalf("disconnect did not preserve last value: %#v", snapshot)
	}
	if store.ApplySubscription(validSample(pointID, oldToken, "late")) {
		t.Fatal("old connection subscription sample was accepted")
	}
	if store.ApplyVerification(Verification{
		PointID: pointID, Value: "last-good", HasValue: true, Validity: SampleValid,
		CheckedAt: time.Now(), Generation: oldToken,
	}, snapshot.Version) {
		t.Fatal("old connection verification was accepted")
	}
}

func TestConnectedStateRunsFullRecoveryWithPartialSubscriptionSuccess(t *testing.T) {
	t.Parallel()

	store := newStateStore(time.Now)
	oldPoints := []PointID{"point.keep", "point.missing"}
	oldTokens := initialTokens(oldPoints, 1)
	if err := store.Initialize(oldPoints, oldTokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	store.ApplySubscription(validSample("point.keep", oldTokens["point.keep"], 10))
	store.ApplySubscription(validSample("point.missing", oldTokens["point.missing"], 20))

	oldSubscription := &fakePointSubscription{}
	newSubscription := &fakePointSubscription{}
	adapter := &fakeSourceAdapter{}
	adapter.rediscoverFn = func(context.Context, uint64) DiscoveryResult {
		return DiscoveryResult{Points: []PointID{"point.new", "point.keep"}}
	}
	adapter.subscribeFn = func(points []PointID, tokens map[PointID]GenerationToken, _ SampleHandler) SubscriptionResult {
		if !reflect.DeepEqual(points, []PointID{"point.keep", "point.new"}) {
			t.Fatalf("recovery subscribed points = %#v", points)
		}
		return SubscriptionResult{
			Succeeded:    []PointID{"point.keep"},
			Failed:       []PointFailure{{PointID: "point.new", Reason: "monitor rejected"}},
			Subscription: newSubscription,
		}
	}
	adapter.verifyFn = func(points []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{Verifications: []Verification{
			{PointID: "point.keep", Value: 10, HasValue: true, Validity: SampleValid, CheckedAt: time.Now(), Generation: tokens["point.keep"]},
			{PointID: "point.new", Value: 30, HasValue: true, Validity: SampleValid, CheckedAt: time.Now(), Generation: tokens["point.new"]},
		}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	var logs bytes.Buffer
	coordinator.logger = jsonTestLogger(&logs)
	coordinator.trackSubscription(oldSubscription)
	nextRecovery := time.Time{}
	backoffIndex := 0
	coordinator.handleConnectionState(context.Background(), SourceDisconnected, time.Now(), &nextRecovery, &backoffIndex)
	coordinator.handleConnectionState(context.Background(), SourceConnected, time.Now(), &nextRecovery, &backoffIndex)
	for _, message := range []string{`"msg":"Source connection unavailable"`, `"msg":"Acquisition recovery started"`, `"msg":"Acquisition recovery completed with unhealthy points"`} {
		if !strings.Contains(logs.String(), message) {
			t.Fatalf("recovery log missing %s: %s", message, logs.String())
		}
	}
	if !strings.Contains(logs.String(), `"current_points":2`) || !strings.Contains(logs.String(), `"healthy_points":1`) ||
		!strings.Contains(logs.String(), `"unhealthy_points":1`) || !strings.Contains(logs.String(), `"subscribe_failed_points":1`) {
		t.Fatalf("recovery success log lost point counts: %s", logs.String())
	}

	if !coordinator.isSourceAvailable() {
		t.Fatal("source remained unavailable after successful recovery")
	}
	if oldSubscription.closeCount() != 1 {
		t.Fatalf("old subscription close count = %d", oldSubscription.closeCount())
	}
	if !reflect.DeepEqual(adapter.rediscoveredGenerations(), []uint64{2}) {
		t.Fatalf("full rediscovery generations = %#v", adapter.rediscoveredGenerations())
	}
	keep, _ := store.Get("point.keep")
	if keep.ConnectionGeneration != 2 || keep.MonitorGeneration != 1 || keep.Health != PointHealthy || keep.Value != 10 {
		t.Fatalf("preserved point was not recovered: %#v", keep)
	}
	if _, exists := store.Get("point.missing"); exists {
		t.Fatal("point missing from complete rediscovery was retained")
	}
	newPoint, exists := store.Get("point.new")
	if !exists || newPoint.Health != PointSubscribeFailed || newPoint.ConnectionGeneration != 2 || newPoint.MonitorGeneration != 1 {
		t.Fatalf("partially failed new point state = %#v, exists=%v", newPoint, exists)
	}
	if store.ApplySubscription(validSample("point.keep", oldTokens["point.keep"], 999)) {
		t.Fatal("old connection notification changed recovered state")
	}
}

func TestRecoveryFailureUsesBoundedExponentialBackoff(t *testing.T) {
	t.Parallel()

	store, _, _ := initializedStore(t)
	adapter := &fakeSourceAdapter{rediscoverFn: func(context.Context, uint64) DiscoveryResult { return DiscoveryResult{} }}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.config.recoveryBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}
	coordinator.markSourceUnavailable("test disconnect")
	base := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
	nextRecovery := time.Time{}
	backoffIndex := 0
	wantDelays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	attemptAt := base
	for index, wantDelay := range wantDelays {
		coordinator.handleConnectionState(context.Background(), SourceConnected, attemptAt, &nextRecovery, &backoffIndex)
		if delay := nextRecovery.Sub(attemptAt); delay != wantDelay {
			t.Fatalf("recovery attempt %d delay=%v, want %v", index+1, delay, wantDelay)
		}
		attemptAt = nextRecovery
	}
	if backoffIndex != 5 {
		t.Fatalf("capped backoff index = %d, want 5", backoffIndex)
	}
	attemptsBeforeEarlyCheck := len(adapter.rediscoveredGenerations())
	coordinator.handleConnectionState(context.Background(), SourceConnected, base.Add(500*time.Millisecond), &nextRecovery, &backoffIndex)
	if got := len(adapter.rediscoveredGenerations()); got != attemptsBeforeEarlyCheck {
		t.Fatalf("recovery ignored backoff, attempts=%d before=%d", got, attemptsBeforeEarlyCheck)
	}
}

func TestConnectionLossDuringRecoveryAdvancesGenerationAgain(t *testing.T) {
	t.Parallel()

	store, pointID, _ := initializedStore(t)
	adapter := &fakeSourceAdapter{}
	adapter.rediscoverFn = func(context.Context, uint64) DiscoveryResult {
		adapter.setConnectionState(SourceDisconnected)
		return DiscoveryResult{Points: []PointID{pointID}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	nextRecovery := time.Time{}
	backoffIndex := 0
	coordinator.handleConnectionState(context.Background(), SourceDisconnected, time.Now(), &nextRecovery, &backoffIndex)
	adapter.setConnectionState(SourceConnected)
	coordinator.handleConnectionState(context.Background(), SourceConnected, time.Now(), &nextRecovery, &backoffIndex)

	snapshot, _ := store.Get(pointID)
	if snapshot.ConnectionGeneration != 3 || snapshot.MonitorGeneration != 0 || snapshot.Health != PointSourceDisconnected {
		t.Fatalf("recovery connection loss did not advance generation: %#v", snapshot)
	}
}

func TestStopCancelsInFlightRecovery(t *testing.T) {
	t.Parallel()

	store, _, _ := initializedStore(t)
	entered := make(chan struct{})
	adapter := &fakeSourceAdapter{}
	adapter.rediscoverFn = func(ctx context.Context, _ uint64) DiscoveryResult {
		close(entered)
		<-ctx.Done()
		return DiscoveryResult{Issues: []DiscoveryIssue{{Reason: ctx.Err().Error()}}}
	}
	coordinator := newTestCoordinator(t, adapter, store)
	coordinator.ctx, coordinator.cancel = context.WithCancel(context.Background())
	coordinator.started = true
	coordinator.markSourceUnavailable("test disconnect")
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- coordinator.recoverConnection(coordinator.ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	select {
	case err := <-recoveryDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not exit after Stop")
	}
}

func newTestCoordinator(t *testing.T, adapter SourceAdapter, store StateStore) *AcquisitionCoordinator {
	t.Helper()
	coordinator, err := NewAcquisitionCoordinator(adapter, store, config.CollectorConfig{
		HeartbeatIntervalSec: 60, StaleThresholdSec: 60, SubscriptionRetryIntervalSec: 60,
		ReadBatchSize: 500, ReadTimeoutSec: 10,
	}, nil)
	if err != nil {
		t.Fatalf("NewAcquisitionCoordinator() error = %v", err)
	}
	return coordinator
}

func jsonTestLogger(output *bytes.Buffer) *zap.Logger {
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	return zap.New(zapcore.NewCore(encoder, zapcore.AddSync(output), zap.DebugLevel))
}

type fakeSourceAdapter struct {
	points       []PointID
	subscription *fakePointSubscription
	discoverFn   func(uint64) DiscoveryResult
	rediscoverFn func(context.Context, uint64) DiscoveryResult
	subscribeFn  func([]PointID, map[PointID]GenerationToken, SampleHandler) SubscriptionResult
	verifyFn     func([]PointID, map[PointID]GenerationToken) VerificationResult
	state        SourceConnectionState

	mu            sync.Mutex
	verifyBatches [][]PointID
	discoveries   []uint64
	rediscoveries []uint64
	closes        int
}

func (f *fakeSourceAdapter) Discover(_ context.Context, generation uint64) DiscoveryResult {
	f.mu.Lock()
	f.discoveries = append(f.discoveries, generation)
	f.mu.Unlock()
	if f.discoverFn != nil {
		return f.discoverFn(generation)
	}
	return DiscoveryResult{Points: append([]PointID(nil), f.points...)}
}

func (f *fakeSourceAdapter) discoveredGenerations() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.discoveries...)
}

func (f *fakeSourceAdapter) Rediscover(ctx context.Context, generation uint64) DiscoveryResult {
	f.mu.Lock()
	f.rediscoveries = append(f.rediscoveries, generation)
	fn := f.rediscoverFn
	points := append([]PointID(nil), f.points...)
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, generation)
	}
	return DiscoveryResult{Points: points}
}

func (f *fakeSourceAdapter) rediscoveredGenerations() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.rediscoveries...)
}

func (f *fakeSourceAdapter) Subscribe(_ context.Context, points []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
	if f.subscription == nil {
		f.subscription = &fakePointSubscription{}
	}
	if f.subscribeFn != nil {
		return f.subscribeFn(points, tokens, handler)
	}
	return SubscriptionResult{}
}

func (f *fakeSourceAdapter) Verify(_ context.Context, points []PointID, tokens map[PointID]GenerationToken) VerificationResult {
	f.mu.Lock()
	f.verifyBatches = append(f.verifyBatches, append([]PointID(nil), points...))
	f.mu.Unlock()
	if f.verifyFn != nil {
		return f.verifyFn(points, tokens)
	}
	return VerificationResult{}
}

func (f *fakeSourceAdapter) ConnectionState() SourceConnectionState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == "" {
		return SourceConnected
	}
	return f.state
}

func (f *fakeSourceAdapter) setConnectionState(state SourceConnectionState) {
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
}

func (f *fakeSourceAdapter) Close(context.Context) error {
	f.mu.Lock()
	f.closes++
	f.mu.Unlock()
	return nil
}

func (f *fakeSourceAdapter) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

func (f *fakeSourceAdapter) verifiedBatches() [][]PointID {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([][]PointID, len(f.verifyBatches))
	for index := range f.verifyBatches {
		result[index] = append([]PointID(nil), f.verifyBatches[index]...)
	}
	return result
}

type fakePointSubscription struct {
	mu        sync.Mutex
	removed   []PointID
	closes    int
	removeFn  func(PointID)
	removeErr error
	closeErr  error
}

func (f *fakePointSubscription) Remove(_ context.Context, pointID PointID) error {
	f.mu.Lock()
	f.removed = append(f.removed, pointID)
	removeFn := f.removeFn
	f.mu.Unlock()
	if removeFn != nil {
		removeFn(pointID)
	}
	return f.removeErr
}

func (f *fakePointSubscription) Close(context.Context) error {
	f.mu.Lock()
	f.closes++
	err := f.closeErr
	f.mu.Unlock()
	return err
}

func (f *fakePointSubscription) removedPoints() []PointID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]PointID(nil), f.removed...)
}

func (f *fakePointSubscription) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

var _ SourceAdapter = (*fakeSourceAdapter)(nil)
var _ PointSubscription = (*fakePointSubscription)(nil)
