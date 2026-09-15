package collector

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
)

func TestDataPointFromSnapshotUsesPointIDAndHealth(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	point := dataPointFromSnapshot(PointSnapshot{
		PointID: "line.temperature", Value: false, HasValue: true,
		ObservedAt: observedAt, Health: PointHealthy,
	})
	if point.NodeID != "line.temperature" || point.Value != false || point.Quality != "Good" || !point.Timestamp.Equal(observedAt) {
		t.Fatalf("healthy data point = %#v", point)
	}
	failed := dataPointFromSnapshot(PointSnapshot{PointID: "line.failed", Health: PointSourceDisconnected})
	if failed.Quality == "Good" {
		t.Fatalf("unhealthy snapshot mapped to Good: %#v", failed)
	}
}

func TestImmediateSnapshotChangesRespectForceHeartbeat(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 1))
	collector := &Collector{config: &config.CollectorConfig{}, store: store}
	known := make(map[PointID]publishedPointState)
	if points := collector.changedDataPoints(known); len(points) != 1 {
		t.Fatalf("initial changed points = %#v", points)
	}
	snapshot, _ := store.Get(pointID)
	store.ApplyVerification(Verification{
		PointID: pointID, Value: 1, HasValue: true, Validity: SampleValid,
		CheckedAt: time.Now(), Generation: token,
	}, snapshot.Version)
	if points := collector.changedDataPoints(known); len(points) != 0 {
		t.Fatalf("verification heartbeat published with force_heartbeat=false: %#v", points)
	}
	updated := validSample(pointID, token, 2)
	updated.ObservedAt = updated.ObservedAt.Add(time.Second)
	updated.ReceivedAt = updated.ReceivedAt.Add(time.Second)
	store.ApplySubscription(updated)
	if points := collector.changedDataPoints(known); len(points) != 1 || points[0].Value != 2 {
		t.Fatalf("subscription change points = %#v", points)
	}

	collector.config.ForceHeartbeat = true
	snapshot, _ = store.Get(pointID)
	store.ApplyVerification(Verification{
		PointID: pointID, Value: 2, HasValue: true, Validity: SampleValid,
		CheckedAt: time.Now(), Generation: token,
	}, snapshot.Version)
	if points := collector.changedDataPoints(known); len(points) != 1 {
		t.Fatalf("verification heartbeat not published with force_heartbeat=true: %#v", points)
	}
}

func TestTimedSnapshotFiltersNonHealthyPoints(t *testing.T) {
	t.Parallel()

	store := newStateStore(time.Now)
	points := []PointID{"healthy", "failed"}
	tokens := initialTokens(points, 1)
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	store.ApplySubscription(validSample("healthy", tokens["healthy"], 0))
	store.MarkPointFailed("failed", tokens["failed"], PointSubscribeFailed, "monitor failed")
	publisher := &fakePublisher{}
	collector := &Collector{
		config: &config.CollectorConfig{BatchSize: 100, FilterBadQuality: true, PublishTimeoutMs: 1000, SubscriptionTopic: "test.data"},
		store:  store, publisher: publisher, ctx: context.Background(), logger: nil,
	}
	collector.publishCurrentSnapshot()
	batches := publisher.snapshot()
	if len(batches) != 1 || len(batches[0].points) != 1 {
		t.Fatalf("published batches = %#v", batches)
	}
	if !reflect.DeepEqual(batches[0].points, []model.DataPoint{{
		NodeID: "healthy", Value: 0, Quality: "Good", Timestamp: batches[0].points[0].Timestamp,
	}}) {
		t.Fatalf("published batches = %#v", batches)
	}
	if batches[0].topic != "test.data" {
		t.Fatalf("published topic = %q", batches[0].topic)
	}
}

func TestPublishBatchesSplits250PointsInto10010050(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	collector := newPublishingCollector(100, publisher)
	points := make([]model.DataPoint, 250)
	for index := range points {
		points[index] = model.DataPoint{NodeID: fmt.Sprintf("point.%03d", index), Value: index, Quality: "Good"}
	}
	collector.publishBatches(points)
	assertBatchSizes(t, publisher.snapshot(), 100, 100, 50)
}

func TestPublishBatchesHandlesNonPositiveBatchSizeDefensively(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	collector := newPublishingCollector(0, publisher)
	collector.publishBatches([]model.DataPoint{{NodeID: "a"}, {NodeID: "b"}, {NodeID: "c"}})
	assertBatchSizes(t, publisher.snapshot(), 3)
}

func TestImmediateMergedChangesUseBatchSize(t *testing.T) {
	t.Parallel()

	store := healthyPointStore(t, 5)
	publisher := &fakePublisher{published: make(chan struct{}, 3)}
	collector := newPublishingCollector(2, publisher)
	collector.store = store
	collector.ctx, collector.cancel = context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		collector.publishImmediate()
		close(done)
	}()
	for index := 0; index < 3; index++ {
		select {
		case <-publisher.published:
		case <-time.After(time.Second):
			collector.cancel()
			t.Fatalf("immediate publish completed %d/3 batches", index)
		}
	}
	collector.cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("immediate publisher did not stop")
	}
	assertBatchSizes(t, publisher.snapshot(), 2, 2, 1)
}

func TestTimedSnapshotUsesBatchSize(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	collector := newPublishingCollector(100, publisher)
	collector.store = healthyPointStore(t, 250)
	collector.publishCurrentSnapshot()
	assertBatchSizes(t, publisher.snapshot(), 100, 100, 50)
}

func TestCollectorStatsSeparatesCurrentPointsFromPublishedPoints(t *testing.T) {
	t.Parallel()

	collector := newPublishingCollector(100, &fakePublisher{})
	collector.store = healthyPointStore(t, 6)
	collector.totalPoints.Store(1074)
	collector.successCount.Store(1074)

	stats := collector.collectStats()
	if stats.CurrentPoints != 6 {
		t.Fatalf("current points = %d, want 6", stats.CurrentPoints)
	}
	if stats.TotalPoints != 1074 || stats.SuccessCount != 1074 {
		t.Fatalf("published counters changed unexpectedly: %#v", stats)
	}
	if stats.HealthyPoints != 6 || stats.UnhealthyPoints != 0 {
		t.Fatalf("healthy counters = %#v", stats)
	}
}

func TestSummarizePointHealthReportsEveryState(t *testing.T) {
	t.Parallel()

	healths := []PointHealth{
		PointInitializing, PointHealthy, PointSampleInvalid, PointSubscribeFailed,
		PointVerificationFailed, PointSubscriptionMismatch, PointSourceDisconnected, PointStale,
	}
	snapshots := make([]PointSnapshot, len(healths))
	for index, health := range healths {
		snapshots[index] = PointSnapshot{PointID: PointID(fmt.Sprintf("point.%d", index)), Health: health}
	}
	stats := summarizePointHealth(snapshots)
	if stats.CurrentPoints != 8 || stats.HealthyPoints != 1 || stats.UnhealthyPoints != 7 ||
		stats.InitializingPoints != 1 || stats.SampleInvalidPoints != 1 || stats.SubscribeFailedPoints != 1 ||
		stats.VerificationFailedPoints != 1 || stats.SubscriptionMismatchPoints != 1 ||
		stats.SourceDisconnectedPoints != 1 || stats.StaleCount != 1 {
		t.Fatalf("unexpected health summary: %#v", stats)
	}
}

func TestFilterBadQualityRunsBeforeBatchSplitting(t *testing.T) {
	t.Parallel()

	store := newStateStore(time.Now)
	points := make([]PointID, 205)
	tokens := make(map[PointID]GenerationToken, len(points))
	for index := range points {
		pointID := PointID(fmt.Sprintf("point.%03d", index))
		points[index] = pointID
		tokens[pointID] = GenerationToken{Connection: 1, Monitor: 1}
	}
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	for index, pointID := range points {
		if index < 150 {
			store.ApplySubscription(validSample(pointID, tokens[pointID], index))
			continue
		}
		store.MarkPointFailed(pointID, tokens[pointID], PointSubscribeFailed, "monitor failed")
	}
	publisher := &fakePublisher{}
	collector := newPublishingCollector(100, publisher)
	collector.config.FilterBadQuality = true
	collector.store = store
	collector.publishCurrentSnapshot()
	assertBatchSizes(t, publisher.snapshot(), 100, 50)
}

func TestCollectorStartsNewAcquisitionPathAndStopsOnce(t *testing.T) {
	t.Parallel()

	now := time.Now()
	adapter := &fakeSourceAdapter{points: []PointID{"line.temperature"}}
	adapter.subscribeFn = func(_ []PointID, tokens map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult {
		handler(PointSample{
			PointID: "line.temperature", Value: 25, HasValue: true, Validity: SampleValid,
			ObservedAt: now, ReceivedAt: now, Generation: tokens["line.temperature"],
		})
		return SubscriptionResult{Succeeded: []PointID{"line.temperature"}, Subscription: adapter.subscription}
	}
	adapter.verifyFn = func(_ []PointID, tokens map[PointID]GenerationToken) VerificationResult {
		return VerificationResult{Verifications: []Verification{{
			PointID: "line.temperature", Value: 25, HasValue: true, Validity: SampleValid,
			CheckedAt: now, Generation: tokens["line.temperature"],
		}}}
	}
	publisher := &fakePublisher{published: make(chan struct{}, 1)}
	collector := New(testCollectorConfig(), adapter, publisher, nil)
	if err := collector.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-publisher.published:
	case <-time.After(time.Second):
		collector.Stop()
		t.Fatal("initial StateStore snapshot was not published")
	}
	collector.Stop()
	collector.Stop()
	if adapter.closeCount() != 0 || adapter.subscription.closeCount() != 1 {
		t.Fatalf("stop cleanup: adapter=%d subscription=%d", adapter.closeCount(), adapter.subscription.closeCount())
	}
	batches := publisher.snapshot()
	if len(batches) == 0 || len(batches[0].points) != 1 || batches[0].points[0].NodeID != "line.temperature" {
		t.Fatalf("published batches = %#v", batches)
	}
}

func testCollectorConfig() *config.CollectorConfig {
	return &config.CollectorConfig{
		WorkerCount: 10, BatchSize: 100, ChannelBufferSize: 10000, PublishTimeoutMs: 1000,
		SubscriptionTopic: "opcua/data", MonitorIntervalSec: 60, StaleThresholdSec: 60,
		HeartbeatIntervalSec: 60, SubscriptionRetryIntervalSec: 60, ReadBatchSize: 500,
		ReadTimeoutSec: 10, PushMode: config.PushModeImmediate, PushIntervalSec: 1,
	}
}

func newPublishingCollector(batchSize int, publisher Publisher) *Collector {
	return &Collector{
		config: &config.CollectorConfig{
			BatchSize: batchSize, PublishTimeoutMs: 1000, SubscriptionTopic: "test.data",
		},
		publisher: publisher,
		ctx:       context.Background(),
		logger:    nil,
	}
}

func healthyPointStore(t *testing.T, count int) StateStore {
	t.Helper()
	store := newStateStore(time.Now)
	points := make([]PointID, count)
	tokens := make(map[PointID]GenerationToken, count)
	for index := range points {
		pointID := PointID(fmt.Sprintf("point.%03d", index))
		points[index] = pointID
		tokens[pointID] = GenerationToken{Connection: 1, Monitor: 1}
	}
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	for index, pointID := range points {
		store.ApplySubscription(validSample(pointID, tokens[pointID], index))
	}
	return store
}

func assertBatchSizes(t *testing.T, batches []publishedBatch, want ...int) {
	t.Helper()
	got := make([]int, len(batches))
	for index, batch := range batches {
		got[index] = len(batch.points)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch sizes = %#v, want %#v", got, want)
	}
}

type publishedBatch struct {
	topic  string
	points []model.DataPoint
}

type fakePublisher struct {
	mu        sync.Mutex
	batches   []publishedBatch
	published chan struct{}
}

func (f *fakePublisher) PublishBatch(_ context.Context, topic string, points []model.DataPoint) error {
	f.mu.Lock()
	f.batches = append(f.batches, publishedBatch{topic: topic, points: append([]model.DataPoint(nil), points...)})
	f.mu.Unlock()
	if f.published != nil {
		select {
		case f.published <- struct{}{}:
		default:
		}
	}
	return nil
}

func (f *fakePublisher) IsConnected() bool { return true }
func (f *fakePublisher) Close()            {}

func (f *fakePublisher) snapshot() []publishedBatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]publishedBatch, len(f.batches))
	for index, batch := range f.batches {
		result[index] = publishedBatch{topic: batch.topic, points: append([]model.DataPoint(nil), batch.points...)}
	}
	return result
}

var _ Publisher = (*fakePublisher)(nil)
