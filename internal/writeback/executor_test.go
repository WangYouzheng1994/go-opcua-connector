package writeback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/opcua"

	"github.com/gopcua/opcua/ua"
)

func TestQueueExecutorCapacityAndNonBlockingRejection(t *testing.T) {
	t.Parallel()

	executor := NewQueueExecutor(&fakeTargetResolver{}, nil)
	if cap(executor.queue) != 500 || itemExecutionTimeout != 5*time.Second {
		t.Fatalf("queue capacity = %d, item timeout = %s", cap(executor.queue), itemExecutionTimeout)
	}
	for index := 0; index < requestQueueCapacity; index++ {
		err := executor.TrySubmit(Request{RequestID: fmt.Sprintf("req-%03d", index)})
		if err != nil {
			t.Fatalf("TrySubmit(%d) error = %v", index, err)
		}
	}

	returned := make(chan error, 1)
	go func() {
		returned <- executor.TrySubmit(Request{RequestID: "overflow"})
	}()
	select {
	case err := <-returned:
		assertErrorCode(t, err, ErrorCodeWriteQueueFull)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("queue-full TrySubmit blocked")
	}
	if len(executor.queue) != requestQueueCapacity {
		t.Fatalf("queue length = %d, want %d", len(executor.queue), requestQueueCapacity)
	}
}

func TestQueueExecutorRunsRequestsFIFOWithSingleWorker(t *testing.T) {
	t.Parallel()

	resolver := newFakeTargetResolver()
	resolver.add("point.1", &fakeExecutionTarget{pointID: "point.1", dataType: "Int32"})
	resolver.add("point.2", &fakeExecutionTarget{pointID: "point.2", dataType: "Int32"})
	completed := make(chan Result, 2)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	if err := executor.TrySubmit(singleItemRequest("request.1", "point.1", json.Number("1"))); err != nil {
		t.Fatalf("submit request.1: %v", err)
	}
	if err := executor.TrySubmit(singleItemRequest("request.2", "point.2", json.Number("2"))); err != nil {
		t.Fatalf("submit request.2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	first := awaitResult(t, completed)
	second := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if first.RequestID != "request.1" || second.RequestID != "request.2" {
		t.Fatalf("completion order = %q, %q", first.RequestID, second.RequestID)
	}
	if !reflect.DeepEqual(resolver.resolvedPointIDs(), []collector.PointID{"point.1", "point.2"}) {
		t.Fatalf("resolve order = %#v", resolver.resolvedPointIDs())
	}
}

func TestQueueExecutorPreservesDuplicatePointIDItemOrder(t *testing.T) {
	t.Parallel()

	target := &fakeExecutionTarget{pointID: "motor.start", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("motor.start", target)
	completed := make(chan Result, 1)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	request := Request{
		RequestID: "duplicate",
		Items: []Item{
			{PointID: "motor.start", Value: true},
			{PointID: "motor.start", Value: false},
		},
	}
	if err := executor.TrySubmit(request); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if !result.Success || len(result.Results) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if !reflect.DeepEqual(target.writtenValues(), []any{true, false}) {
		t.Fatalf("write values = %#v", target.writtenValues())
	}
	if target.maxConcurrentWrites() != 1 {
		t.Fatalf("max concurrent writes = %d, want 1", target.maxConcurrentWrites())
	}
}

func TestQueueExecutorItemTimeoutContinuesWithNextItem(t *testing.T) {
	t.Parallel()

	timedOut := &fakeExecutionTarget{pointID: "slow", dataType: "Boolean", waitForWriteContext: true}
	succeeded := &fakeExecutionTarget{pointID: "fast", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("slow", timedOut)
	resolver.add("fast", succeeded)
	completed := make(chan Result, 1)
	executor := newQueueExecutor(resolver, NewValuePreparer(), func(result Result) { completed <- result }, 25*time.Millisecond)
	request := Request{
		RequestID: "timeout",
		Items: []Item{
			{PointID: "slow", Value: true},
			{PointID: "fast", Value: false},
		},
	}
	if err := executor.TrySubmit(request); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if result.Success || len(result.Results) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.Results[0].Code != ErrorCodeWriteTimeout || !result.Results[1].Success {
		t.Fatalf("item results = %#v", result.Results)
	}
	if timedOut.writeCountValue() != 1 || succeeded.writeCountValue() != 1 {
		t.Fatalf("write counts = %d, %d; want 1, 1", timedOut.writeCountValue(), succeeded.writeCountValue())
	}
	if !timedOut.sawWriteCancellation() {
		t.Fatal("timed-out Write did not observe context cancellation")
	}
}

func TestQueueExecutorDataTypeTimeoutCoversWholeItem(t *testing.T) {
	t.Parallel()

	timedOut := &fakeExecutionTarget{pointID: "slow-read", waitForReadContext: true}
	resolver := newFakeTargetResolver()
	resolver.add("slow-read", timedOut)
	completed := make(chan Result, 1)
	executor := newQueueExecutor(resolver, NewValuePreparer(), func(result Result) { completed <- result }, 25*time.Millisecond)
	if err := executor.TrySubmit(singleItemRequest("read-timeout", "slow-read", true)); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if len(result.Results) != 1 || result.Results[0].Code != ErrorCodeWriteTimeout {
		t.Fatalf("result = %#v", result)
	}
	if timedOut.writeCountValue() != 0 || !timedOut.sawReadCancellation() {
		t.Fatalf("write count = %d, read cancellation = %v", timedOut.writeCountValue(), timedOut.sawReadCancellation())
	}
}

func TestQueueExecutorLateWriteReturnIsTimeoutWithoutRetry(t *testing.T) {
	t.Parallel()

	late := &fakeExecutionTarget{pointID: "late", dataType: "Boolean", writeDelay: 40 * time.Millisecond}
	next := &fakeExecutionTarget{pointID: "next", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("late", late)
	resolver.add("next", next)
	completed := make(chan Result, 1)
	executor := newQueueExecutor(resolver, NewValuePreparer(), func(result Result) { completed <- result }, 10*time.Millisecond)
	request := Request{
		RequestID: "late-write",
		Items: []Item{
			{PointID: "late", Value: true},
			{PointID: "next", Value: false},
		},
	}
	if err := executor.TrySubmit(request); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if result.Results[0].Code != ErrorCodeWriteTimeout || !result.Results[1].Success {
		t.Fatalf("item results = %#v", result.Results)
	}
	if late.writeCountValue() != 1 || next.writeCountValue() != 1 {
		t.Fatalf("write counts = %d, %d; want 1, 1", late.writeCountValue(), next.writeCountValue())
	}
}

func TestQueueExecutorFailureContinuesAndWritesAtMostOnce(t *testing.T) {
	t.Parallel()

	conversionFailure := &fakeExecutionTarget{pointID: "invalid", dataType: "Int32"}
	writeFailure := &fakeExecutionTarget{pointID: "write-failure", dataType: "Boolean", writeErr: errors.New("rejected")}
	success := &fakeExecutionTarget{pointID: "success", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("invalid", conversionFailure)
	resolver.add("write-failure", writeFailure)
	resolver.add("success", success)
	completed := make(chan Result, 1)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	request := Request{
		RequestID: "continue",
		Items: []Item{
			{PointID: "invalid", Value: json.Number("1.5")},
			{PointID: "write-failure", Value: true},
			{PointID: "success", Value: false},
		},
	}
	if err := executor.TrySubmit(request); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if result.Success || len(result.Results) != 3 {
		t.Fatalf("result = %#v", result)
	}
	if result.Results[0].Code != ErrorCodeValueConversionFailed || result.Results[1].Code != ErrorCodeWriteFailed || !result.Results[2].Success {
		t.Fatalf("item results = %#v", result.Results)
	}
	if conversionFailure.writeCountValue() != 0 || writeFailure.writeCountValue() != 1 || success.writeCountValue() != 1 {
		t.Fatalf("write counts = %d, %d, %d", conversionFailure.writeCountValue(), writeFailure.writeCountValue(), success.writeCountValue())
	}
}

func TestQueueExecutorMapsMissingPointAndContinues(t *testing.T) {
	t.Parallel()

	success := &fakeExecutionTarget{pointID: "success", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("success", success)
	completed := make(chan Result, 1)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	request := Request{
		RequestID: "missing",
		Items: []Item{
			{PointID: "missing", Value: true},
			{PointID: "success", Value: true},
		},
	}
	if err := executor.TrySubmit(request); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if result.Results[0].Code != ErrorCodePointNotFound || !result.Results[1].Success {
		t.Fatalf("item results = %#v", result.Results)
	}
}

func TestQueueExecutorMapsOPCUABadStatusAsWriteFailedDiagnostic(t *testing.T) {
	t.Parallel()

	target := &fakeExecutionTarget{
		pointID:  "readonly",
		dataType: "Boolean",
		writeErr: ua.StatusBadNotWritable,
	}
	resolver := newFakeTargetResolver()
	resolver.add("readonly", target)
	completed := make(chan Result, 1)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	if err := executor.TrySubmit(singleItemRequest("bad-status", "readonly", true)); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go executor.Run(ctx)
	result := awaitResult(t, completed)
	cancel()
	awaitDone(t, executor.Done())

	if len(result.Results) != 1 {
		t.Fatalf("result = %#v", result)
	}
	item := result.Results[0]
	if item.Code != ErrorCodeWriteFailed || item.OPCUAStatus != "BadNotWritable" {
		t.Fatalf("item result = %#v", item)
	}
	if target.writeCountValue() != 1 {
		t.Fatalf("write count = %d, want 1", target.writeCountValue())
	}
}

func TestQueueExecutorShutdownBeforeRunReturnsImmediately(t *testing.T) {
	t.Parallel()

	executor := NewQueueExecutor(&fakeTargetResolver{}, nil)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	startedAt := time.Now()
	if err := executor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed >= 100*time.Millisecond {
		t.Fatalf("Shutdown() before Run took %s", elapsed)
	}
	awaitDone(t, executor.Done())
	if !errors.Is(executor.TrySubmit(Request{RequestID: "late"}), errExecutorStopped) {
		t.Fatal("TrySubmit() after shutdown did not return errExecutorStopped")
	}

	// Run 在 Shutdown 后调用只能返回，不能重新打开 Worker 或重复关闭 Done。
	executor.Run(context.Background())
	awaitDone(t, executor.Done())
}

func TestQueueExecutorRunShutdownRaceStopsBeforeConsumingQueue(t *testing.T) {
	t.Parallel()

	queued := &fakeExecutionTarget{pointID: "queued", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("queued", queued)
	executor := NewQueueExecutor(resolver, nil)
	if err := executor.TrySubmit(singleItemRequest("queued", "queued", true)); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	// 暂时持有生命周期锁，让 Run 和 Shutdown 同时到达状态判定边界。
	executor.lifecycleMu.Lock()
	start := make(chan struct{})
	runReturned := make(chan struct{})
	shutdownReturned := make(chan error, 1)
	go func() {
		<-start
		executor.Run(context.Background())
		close(runReturned)
	}()
	go func() {
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownReturned <- executor.Shutdown(ctx)
	}()
	close(start)

	select {
	case <-executor.stopping:
	case <-time.After(time.Second):
		executor.lifecycleMu.Unlock()
		t.Fatal("Shutdown did not stop acceptance")
	}
	executor.lifecycleMu.Unlock()

	select {
	case <-runReturned:
	case <-time.After(time.Second):
		t.Fatal("Run deadlocked with Shutdown")
	}
	select {
	case err := <-shutdownReturned:
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown deadlocked with Run")
	}
	awaitDone(t, executor.Done())
	if queued.writeCountValue() != 0 {
		t.Fatalf("queued write count = %d, want 0", queued.writeCountValue())
	}
	if !errors.Is(executor.TrySubmit(singleItemRequest("late", "queued", true)), errExecutorStopped) {
		t.Fatal("TrySubmit() after shutdown did not return errExecutorStopped")
	}
}

func TestQueueExecutorShutdownFinishesCurrentRequestAndDiscardsQueue(t *testing.T) {
	t.Parallel()

	current := &fakeExecutionTarget{pointID: "current", dataType: "Boolean", writeDelay: 40 * time.Millisecond}
	queued := &fakeExecutionTarget{pointID: "queued", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("current", current)
	resolver.add("queued", queued)
	completed := make(chan Result, 2)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	if err := executor.TrySubmit(singleItemRequest("current", "current", true)); err != nil {
		t.Fatalf("submit current request: %v", err)
	}
	if err := executor.TrySubmit(singleItemRequest("queued", "queued", true)); err != nil {
		t.Fatalf("submit queued request: %v", err)
	}

	go executor.Run(context.Background())
	waitForWriteCount(t, current, 1)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	result := awaitResult(t, completed)
	if result.RequestID != "current" || !result.Success {
		t.Fatalf("completed result = %#v", result)
	}
	select {
	case extra := <-completed:
		t.Fatalf("queued request unexpectedly completed: %#v", extra)
	default:
	}
	if queued.writeCountValue() != 0 {
		t.Fatalf("queued write count = %d, want 0", queued.writeCountValue())
	}
	if !errors.Is(executor.TrySubmit(singleItemRequest("late", "queued", true)), errExecutorStopped) {
		t.Fatal("TrySubmit() after shutdown did not return errExecutorStopped")
	}
}

func TestQueueExecutorShutdownDeadlineCancelsCurrentWithoutRetry(t *testing.T) {
	t.Parallel()

	target := &fakeExecutionTarget{pointID: "blocked", dataType: "Boolean", waitForWriteContext: true}
	resolver := newFakeTargetResolver()
	resolver.add("blocked", target)
	completed := make(chan Result, 1)
	executor := NewQueueExecutor(resolver, func(result Result) { completed <- result })
	if err := executor.TrySubmit(singleItemRequest("blocked", "blocked", true)); err != nil {
		t.Fatalf("TrySubmit() error = %v", err)
	}

	go executor.Run(context.Background())
	waitForWriteCount(t, target, 1)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := executor.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want context deadline exceeded", err)
	}
	awaitDone(t, executor.Done())
	result := awaitResult(t, completed)
	if len(result.Results) != 1 || result.Results[0].Code != ErrorCodeWriteTimeout {
		t.Fatalf("result = %#v", result)
	}
	if target.writeCountValue() != 1 || !target.sawWriteCancellation() {
		t.Fatalf("write count = %d, cancellation = %v", target.writeCountValue(), target.sawWriteCancellation())
	}
}

func singleItemRequest(requestID string, pointID collector.PointID, value any) Request {
	return Request{
		RequestID: requestID,
		Items:     []Item{{PointID: pointID, Value: value}},
	}
}

func awaitResult(t *testing.T, completed <-chan Result) Result {
	t.Helper()
	select {
	case result := <-completed:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request completion")
		return Result{}
	}
}

func awaitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for executor to stop")
	}
}

func waitForWriteCount(t *testing.T, target *fakeExecutionTarget, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if target.writeCountValue() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("write count = %d, want at least %d", target.writeCountValue(), want)
}

type fakeTargetResolver struct {
	mu      sync.Mutex
	targets map[collector.PointID]opcua.WriteTarget
	order   []collector.PointID
}

func newFakeTargetResolver() *fakeTargetResolver {
	return &fakeTargetResolver{targets: make(map[collector.PointID]opcua.WriteTarget)}
}

func (r *fakeTargetResolver) add(pointID collector.PointID, target opcua.WriteTarget) {
	r.targets[pointID] = target
}

func (r *fakeTargetResolver) ResolveWriteTarget(pointID collector.PointID) (opcua.WriteTarget, error) {
	r.mu.Lock()
	r.order = append(r.order, pointID)
	target, ok := r.targets[pointID]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", opcua.ErrPointNotFound, pointID)
	}
	return target, nil
}

func (r *fakeTargetResolver) resolvedPointIDs() []collector.PointID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]collector.PointID(nil), r.order...)
}

type fakeExecutionTarget struct {
	pointID             collector.PointID
	dataType            string
	readErr             error
	writeErr            error
	waitForReadContext  bool
	waitForWriteContext bool
	writeDelay          time.Duration

	mu              sync.Mutex
	writes          []any
	writeCount      int
	activeWrites    int
	maxActiveWrites int
	readCancelled   bool
	writeCancelled  bool
}

func (t *fakeExecutionTarget) PointID() collector.PointID {
	return t.pointID
}

func (t *fakeExecutionTarget) ReadDataType(ctx context.Context) (string, error) {
	if t.waitForReadContext {
		<-ctx.Done()
		t.mu.Lock()
		t.readCancelled = true
		t.mu.Unlock()
		return "", ctx.Err()
	}
	return t.dataType, t.readErr
}

func (t *fakeExecutionTarget) Write(ctx context.Context, value any) error {
	t.mu.Lock()
	t.writeCount++
	t.activeWrites++
	if t.activeWrites > t.maxActiveWrites {
		t.maxActiveWrites = t.activeWrites
	}
	t.writes = append(t.writes, value)
	t.mu.Unlock()

	if t.waitForWriteContext {
		<-ctx.Done()
		t.mu.Lock()
		t.writeCancelled = true
		t.activeWrites--
		t.mu.Unlock()
		return ctx.Err()
	}
	if t.writeDelay > 0 {
		time.Sleep(t.writeDelay)
	}

	t.mu.Lock()
	t.activeWrites--
	t.mu.Unlock()
	return t.writeErr
}

func (t *fakeExecutionTarget) writtenValues() []any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]any(nil), t.writes...)
}

func (t *fakeExecutionTarget) writeCountValue() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writeCount
}

func (t *fakeExecutionTarget) maxConcurrentWrites() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxActiveWrites
}

func (t *fakeExecutionTarget) sawReadCancellation() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readCancelled
}

func (t *fakeExecutionTarget) sawWriteCancellation() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writeCancelled
}
