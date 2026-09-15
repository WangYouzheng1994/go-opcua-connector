package collector

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestStateStoreInitialValuesPreserveZeroValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "zero", value: 0},
		{name: "false", value: false},
		{name: "empty string", value: ""},
	}

	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			store, pointID, token := initializedStore(t)
			before, _ := store.Get(pointID)
			at := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)

			applied := store.SeedInitialValue(Verification{
				PointID:    pointID,
				Value:      testCase.value,
				HasValue:   true,
				Validity:   SampleValid,
				ObservedAt: at,
				CheckedAt:  at,
				Generation: token,
			}, before.Version)
			if !applied {
				t.Fatal("expected initial value to be applied")
			}

			snapshot, _ := store.Get(pointID)
			if !snapshot.HasValue || !reflect.DeepEqual(snapshot.Value, testCase.value) {
				t.Fatalf("unexpected snapshot value: %#v", snapshot)
			}
		})
	}
}

func TestStateStoreRejectsStaleVerificationVersion(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	beforeRead, _ := store.Get(pointID)
	if !store.ApplySubscription(validSample(pointID, token, "subscription")) {
		t.Fatal("expected subscription sample")
	}
	if store.ApplyVerification(Verification{
		PointID: pointID, Value: "read", HasValue: true, Validity: SampleValid,
		CheckedAt: time.Now(), Generation: token,
	}, beforeRead.Version) {
		t.Fatal("verification started before a subscription update must be rejected")
	}

	snapshot, _ := store.Get(pointID)
	if snapshot.Value != "subscription" || snapshot.Health != PointHealthy {
		t.Fatalf("stale verification changed state: %#v", snapshot)
	}
}

func TestStateStoreOlderSubscriptionDoesNotChangeBusinessState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sample func(PointID, GenerationToken, time.Time, time.Time) PointSample
	}{
		{
			name: "valid",
			sample: func(pointID PointID, token GenerationToken, observedAt, receivedAt time.Time) PointSample {
				return PointSample{
					PointID: pointID, Value: "old", HasValue: true, Validity: SampleValid,
					ObservedAt: observedAt, ReceivedAt: receivedAt, Generation: token,
				}
			},
		},
		{
			name: "invalid",
			sample: func(pointID PointID, token GenerationToken, observedAt, receivedAt time.Time) PointSample {
				return PointSample{
					PointID: pointID, HasValue: false, Validity: SampleInvalid,
					ObservedAt: observedAt, ReceivedAt: receivedAt,
					FailureReason: "old invalid sample", Generation: token,
				}
			},
		},
	}

	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store, pointID, token := initializedStore(t)
			newer := time.Date(2026, 9, 10, 10, 0, 2, 0, time.UTC)
			older := newer.Add(-time.Second)
			if !store.ApplySubscription(PointSample{
				PointID: pointID, Value: "new", HasValue: true, Validity: SampleValid,
				ObservedAt: newer, ReceivedAt: newer, Generation: token,
			}) {
				t.Fatal("expected newer sample")
			}
			before, _ := store.Get(pointID)
			drainChangeSignal(store.SubscribeChanges())

			if !store.ApplySubscription(testCase.sample(pointID, token, older, newer.Add(time.Second))) {
				t.Fatal("current-generation older sample should be recognized and ignored")
			}
			after, _ := store.Get(pointID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("older sample changed business state:\nbefore: %#v\nafter:  %#v", before, after)
			}
			select {
			case <-store.SubscribeChanges():
				t.Fatal("older sample must not publish a state change signal")
			default:
			}
		})
	}
}

func TestStateStoreSubscriptionOwnsRuntimeValue(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	t1 := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	initial, _ := store.Get(pointID)
	if !store.SeedInitialValue(Verification{
		PointID: pointID, Value: 10, HasValue: true, Validity: SampleValid,
		ObservedAt: t1, CheckedAt: t1, Generation: token,
	}, initial.Version) {
		t.Fatal("expected initial seed to succeed")
	}
	if !store.ApplySubscription(PointSample{
		PointID: pointID, Value: 20, HasValue: true, Validity: SampleValid,
		ObservedAt: t2, ReceivedAt: t2, Generation: token,
	}) {
		t.Fatal("expected subscription sample to succeed")
	}

	beforeVerification, _ := store.Get(pointID)
	if !store.ApplyVerification(Verification{
		PointID: pointID, Value: 99, HasValue: true, Validity: SampleValid,
		ObservedAt: t3, CheckedAt: t3, Generation: token,
	}, beforeVerification.Version) {
		t.Fatal("expected verification to be evaluated")
	}

	afterVerification, _ := store.Get(pointID)
	if afterVerification.Value != 20 || !afterVerification.ObservedAt.Equal(t2) {
		t.Fatalf("verification changed runtime value: %#v", afterVerification)
	}
	if afterVerification.Health != PointSubscriptionMismatch || afterVerification.Validity != SampleInvalid {
		t.Fatalf("expected mismatch state, got health=%s validity=%s", afterVerification.Health, afterVerification.Validity)
	}

	if store.SeedInitialValue(Verification{
		PointID: pointID, Value: 30, HasValue: true, Validity: SampleValid,
		ObservedAt: t3, CheckedAt: t3, Generation: token,
	}, afterVerification.Version) {
		t.Fatal("initial read must not regain write authority")
	}
}

func TestStateStoreVerificationTransitions(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	at := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	if !store.ApplySubscription(PointSample{
		PointID: pointID, Value: 10, HasValue: true, Validity: SampleValid,
		ObservedAt: at, ReceivedAt: at, Generation: token,
	}) {
		t.Fatal("expected subscription sample")
	}
	if !store.MarkPointFailed(pointID, token, PointStale, "confirmation expired") {
		t.Fatal("expected stale state")
	}

	stale, _ := store.Get(pointID)
	confirmedAt := at.Add(time.Second)
	if !store.ApplyVerification(Verification{
		PointID: pointID, Value: 10, HasValue: true, Validity: SampleValid,
		CheckedAt: confirmedAt, Generation: token,
	}, stale.Version) {
		t.Fatal("expected matching verification")
	}
	healthy, _ := store.Get(pointID)
	if healthy.Health != PointHealthy || healthy.Validity != SampleValid || !healthy.LastConfirmedAt.Equal(confirmedAt) {
		t.Fatalf("matching verification did not restore health: %#v", healthy)
	}
	if healthy.Value != 10 || !healthy.ObservedAt.Equal(at) {
		t.Fatalf("matching verification changed value fields: %#v", healthy)
	}

	failedAt := confirmedAt.Add(time.Second)
	if !store.ApplyVerification(Verification{
		PointID: pointID, HasValue: false, Validity: SampleInvalid,
		CheckedAt: failedAt, FailureReason: "read failed", Generation: token,
	}, healthy.Version) {
		t.Fatal("expected failed verification")
	}
	failed, _ := store.Get(pointID)
	if failed.Health != PointVerificationFailed || failed.FailureReason != "read failed" {
		t.Fatalf("unexpected verification failure state: %#v", failed)
	}
	if failed.Value != 10 || !failed.ObservedAt.Equal(at) {
		t.Fatalf("failed verification changed value fields: %#v", failed)
	}
}

func TestStateStoreVerificationCannotCreateValue(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	initial, _ := store.Get(pointID)
	if !store.ApplyVerification(Verification{
		PointID: pointID, Value: 10, HasValue: true, Validity: SampleValid,
		CheckedAt: time.Now(), Generation: token,
	}, initial.Version) {
		t.Fatal("expected verification to be recorded")
	}

	snapshot, _ := store.Get(pointID)
	if snapshot.HasValue || snapshot.Value != nil {
		t.Fatalf("verification created a runtime value: %#v", snapshot)
	}
	if snapshot.Health != PointVerificationFailed {
		t.Fatalf("expected verification failure, got %s", snapshot.Health)
	}
}

func TestStateStoreInitializationValidationIsAtomic(t *testing.T) {
	t.Parallel()

	store := NewStateStore()
	err := store.Initialize(
		[]PointID{"valid", ""},
		map[PointID]GenerationToken{
			"valid": {Connection: 1, Monitor: 1},
			"":      {Connection: 1, Monitor: 1},
		},
	)
	if err == nil {
		t.Fatal("expected invalid point ID error")
	}
	if snapshot := store.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("failed initialization partially changed state: %#v", snapshot)
	}
}

func TestStateStoreInvalidSamplesPreserveLastValue(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	t1 := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	if !store.ApplySubscription(PointSample{
		PointID: pointID, Value: "trusted", HasValue: true, Validity: SampleValid,
		ObservedAt: t1, ReceivedAt: t1, Generation: token,
	}) {
		t.Fatal("expected valid subscription sample")
	}
	if !store.ApplySubscription(PointSample{
		PointID: pointID, HasValue: false, Validity: SampleInvalid,
		ReceivedAt: t2, FailureReason: "adapter diagnostic", Generation: token,
	}) {
		t.Fatal("expected invalid sample to update diagnostics")
	}

	snapshot, _ := store.Get(pointID)
	if snapshot.Value != "trusted" || !snapshot.ObservedAt.Equal(t1) {
		t.Fatalf("invalid sample replaced last value: %#v", snapshot)
	}
	if snapshot.Health != PointSampleInvalid || snapshot.FailureReason != "adapter diagnostic" {
		t.Fatalf("unexpected diagnostic state: %#v", snapshot)
	}

	// FailureReason 的内容不同不改变控制路径；有效订阅样本始终按结构化字段恢复状态。
	if !store.ApplySubscription(PointSample{
		PointID: pointID, Value: "recovered", HasValue: true, Validity: SampleValid,
		ObservedAt: t2, ReceivedAt: t2, FailureReason: "must not control recovery", Generation: token,
	}) {
		t.Fatal("expected recovery sample")
	}
	snapshot, _ = store.Get(pointID)
	if snapshot.Health != PointHealthy || snapshot.FailureReason != "" || snapshot.Value != "recovered" {
		t.Fatalf("diagnostic text affected recovery: %#v", snapshot)
	}
}

func TestStateStoreVerificationCannotClearSubscriptionFailure(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	if !store.MarkPointFailed(pointID, token, PointSubscribeFailed, "monitor creation failed") {
		t.Fatal("expected subscription failure state")
	}
	failed, _ := store.Get(pointID)
	at := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	if !store.SeedInitialValue(Verification{
		PointID: pointID, Value: 10, HasValue: true, Validity: SampleValid,
		ObservedAt: at, CheckedAt: at, Generation: token,
	}, failed.Version) {
		t.Fatal("expected initial value to be retained for diagnostics")
	}

	seeded, _ := store.Get(pointID)
	if seeded.Health != PointSubscribeFailed || seeded.FailureReason != "monitor creation failed" {
		t.Fatalf("initial read cleared subscription failure: %#v", seeded)
	}
	if !seeded.HasValue || seeded.Value != 10 {
		t.Fatalf("initial value was not retained: %#v", seeded)
	}

	if !store.ApplySubscription(PointSample{
		PointID: pointID, HasValue: false, Validity: SampleInvalid,
		ReceivedAt: at.Add(time.Second), FailureReason: "invalid subscription sample", Generation: token,
	}) {
		t.Fatal("expected invalid subscription sample")
	}
	invalid, _ := store.Get(pointID)
	if !store.ApplyVerification(Verification{
		PointID: pointID, Value: 10, HasValue: true, Validity: SampleValid,
		CheckedAt: at.Add(2 * time.Second), Generation: token,
	}, invalid.Version) {
		t.Fatal("expected verification to be recorded")
	}
	afterVerification, _ := store.Get(pointID)
	if afterVerification.Health != PointSampleInvalid || afterVerification.FailureReason != "invalid subscription sample" {
		t.Fatalf("verification cleared subscription sample failure: %#v", afterVerification)
	}
}

func TestStateStoreRejectsOldMonitorGenerationOnlyForTargetPoint(t *testing.T) {
	t.Parallel()

	store := newStateStore(time.Now)
	pointA, pointB := PointID("a"), PointID("b")
	tokenA := GenerationToken{Connection: 1, Monitor: 1}
	tokenB := GenerationToken{Connection: 1, Monitor: 1}
	if err := store.Initialize(
		[]PointID{pointA, pointB},
		map[PointID]GenerationToken{pointA: tokenA, pointB: tokenB},
	); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	newTokenA, ok := store.AdvanceMonitor(pointA, tokenA, PointSubscribeFailed)
	if !ok || newTokenA != (GenerationToken{Connection: 1, Monitor: 2}) {
		t.Fatalf("unexpected new monitor token: %#v, %v", newTokenA, ok)
	}
	if store.ApplySubscription(validSample(pointA, tokenA, 1)) {
		t.Fatal("old monitor generation must be rejected")
	}
	if !store.ApplySubscription(validSample(pointB, tokenB, 2)) {
		t.Fatal("advancing point A must not invalidate point B")
	}
	if !store.ApplySubscription(validSample(pointA, newTokenA, 3)) {
		t.Fatal("new monitor generation should be accepted")
	}
}

func TestStateStoreConnectionGenerationInvalidatesAllOldResults(t *testing.T) {
	t.Parallel()

	store, pointID, oldToken := initializedStore(t)
	if !store.ApplySubscription(validSample(pointID, oldToken, "old")) {
		t.Fatal("expected initial subscription sample")
	}

	nextConnection, ok := store.AdvanceConnection(1, "connection lost")
	if !ok || nextConnection != 2 {
		t.Fatalf("unexpected connection generation: %d, %v", nextConnection, ok)
	}
	if store.ApplySubscription(validSample(pointID, oldToken, "late")) {
		t.Fatal("old connection result must be rejected immediately")
	}
	disconnected, _ := store.Get(pointID)
	if disconnected.Health != PointSourceDisconnected || disconnected.Value != "old" || disconnected.MonitorGeneration != 0 {
		t.Fatalf("unexpected disconnected state: %#v", disconnected)
	}

	newToken := GenerationToken{Connection: 2, Monitor: 1}
	if err := store.Initialize([]PointID{pointID}, map[PointID]GenerationToken{pointID: newToken}); err != nil {
		t.Fatalf("reinitialize: %v", err)
	}
	reinitialized, _ := store.Get(pointID)
	if reinitialized.Value != "old" || !reinitialized.HasValue {
		t.Fatalf("reinitialize should preserve the last trusted value: %#v", reinitialized)
	}
	if !store.ApplySubscription(validSample(pointID, newToken, "new")) {
		t.Fatal("new connection result should be accepted")
	}
}

func TestStateStoreRemoveRejectsLateResult(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	if !store.Remove(pointID, token) {
		t.Fatal("expected point removal")
	}
	if store.ApplySubscription(validSample(pointID, token, "late")) {
		t.Fatal("late result must not recreate a removed point")
	}
	if _, exists := store.Get(pointID); exists {
		t.Fatal("removed point unexpectedly exists")
	}
}

func TestStateStoreRejectsInvalidLifecycleHealth(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	if store.MarkPointFailed(pointID, token, PointHealth("Unknown"), "diagnostic") {
		t.Fatal("unknown health must be rejected")
	}
	if _, ok := store.AdvanceMonitor(pointID, token, PointHealthy); ok {
		t.Fatal("a new monitor lifecycle cannot start as healthy")
	}

	snapshot, _ := store.Get(pointID)
	if snapshot.Health != PointInitializing || snapshot.Version != 1 {
		t.Fatalf("rejected lifecycle transition changed state: %#v", snapshot)
	}
}

func TestStateStoreClonesMutableValues(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	input := map[string]any{
		"numbers": []int{1, 2},
		"bytes":   []byte{3, 4},
	}
	if !store.ApplySubscription(validSample(pointID, token, input)) {
		t.Fatal("expected subscription sample")
	}
	input["numbers"].([]int)[0] = 99
	input["bytes"].([]byte)[0] = 99

	first, _ := store.Get(pointID)
	firstValue := first.Value.(map[string]any)
	if firstValue["numbers"].([]int)[0] != 1 || firstValue["bytes"].([]byte)[0] != 3 {
		t.Fatalf("stored value shares input memory: %#v", firstValue)
	}
	firstValue["numbers"].([]int)[0] = 88
	firstValue["bytes"].([]byte)[0] = 88

	second, _ := store.Get(pointID)
	secondValue := second.Value.(map[string]any)
	if secondValue["numbers"].([]int)[0] != 1 || secondValue["bytes"].([]byte)[0] != 3 {
		t.Fatalf("snapshot leaked mutable state: %#v", secondValue)
	}
}

func TestStateStoreSnapshotIsSortedAndConcurrentSafe(t *testing.T) {
	t.Parallel()

	store := newStateStore(time.Now)
	points := []PointID{"c", "a", "b"}
	tokens := map[PointID]GenerationToken{}
	for _, pointID := range points {
		tokens[pointID] = GenerationToken{Connection: 1, Monitor: 1}
	}
	if err := store.Initialize(points, tokens); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	var wg sync.WaitGroup
	for _, pointID := range points {
		pointID := pointID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.ApplySubscription(validSample(pointID, tokens[pointID], i))
				_ = store.Snapshot()
			}
		}()
	}
	wg.Wait()

	snapshot := store.Snapshot()
	wantOrder := []PointID{"a", "b", "c"}
	for i, want := range wantOrder {
		if snapshot[i].PointID != want {
			t.Fatalf("snapshot is not sorted: %#v", snapshot)
		}
	}
}

func TestStateStoreChangeSignalIsCoalesced(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 1))
	store.ApplySubscription(validSample(pointID, token, 2))

	select {
	case <-store.SubscribeChanges():
	default:
		t.Fatal("expected change signal")
	}
	select {
	case <-store.SubscribeChanges():
		t.Fatal("change signals should be coalesced")
	default:
	}
}

func TestMarkPointFailedIfVersionDoesNotOverwriteNewSubscriptionSample(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	before, _ := store.Get(pointID)
	if !store.ApplySubscription(validSample(pointID, token, 42)) {
		t.Fatal("subscription sample was rejected")
	}
	if store.MarkPointFailedIfVersion(pointID, token, before.Version, PointStale, "expired snapshot") {
		t.Fatal("stale snapshot overwrote a newer subscription sample")
	}
	after, _ := store.Get(pointID)
	if after.Health != PointHealthy || after.Value != 42 {
		t.Fatalf("new state was changed: %#v", after)
	}
}

func TestMarkPointMonitoringKeepsLastValue(t *testing.T) {
	t.Parallel()

	store, pointID, token := initializedStore(t)
	store.ApplySubscription(validSample(pointID, token, 42))
	newToken, ok := store.AdvanceMonitor(pointID, token, PointSubscribeFailed)
	if !ok || !store.MarkPointMonitoring(pointID, newToken) {
		t.Fatal("failed to enter monitoring state")
	}
	after, _ := store.Get(pointID)
	if after.Health != PointInitializing || !after.HasValue || after.Value != 42 || after.Validity != SampleInvalid {
		t.Fatalf("unexpected monitoring state: %#v", after)
	}
}

func initializedStore(t *testing.T) (*memoryStateStore, PointID, GenerationToken) {
	t.Helper()
	store := newStateStore(time.Now)
	pointID := PointID("machine.temperature")
	token := GenerationToken{Connection: 1, Monitor: 1}
	if err := store.Initialize([]PointID{pointID}, map[PointID]GenerationToken{pointID: token}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	return store, pointID, token
}

func validSample(pointID PointID, token GenerationToken, value any) PointSample {
	at := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)
	return PointSample{
		PointID: pointID, Value: value, HasValue: true, Validity: SampleValid,
		ObservedAt: at, ReceivedAt: at, Generation: token,
	}
}

func drainChangeSignal(ch <-chan struct{}) {
	select {
	case <-ch:
	default:
	}
}
