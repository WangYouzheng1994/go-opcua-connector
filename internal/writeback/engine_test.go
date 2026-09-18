package writeback

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go-opcua-connector/internal/config"

	"go.uber.org/zap"
)

func TestEnginePublishesInvalidRequestWithRecoverableRequestID(t *testing.T) {
	t.Parallel()

	transport := newFakeWritebackTransport()
	engine := NewEngine(nil, transport, testWritebackConfig(), zap.NewNop())
	engine.handleMessage([]byte(`{"request_id":"req-invalid","items":[]}`))
	engine.handleMessage([]byte(`{"request_id":"req-broken"`))

	first := decodePublishedResult(t, transport.awaitPublished(t))
	second := decodePublishedResult(t, transport.awaitPublished(t))
	if first.RequestID != "req-invalid" || first.Code != ErrorCodeInvalidRequest || first.Success || len(first.Results) != 0 {
		t.Fatalf("first result = %#v", first)
	}
	if second.RequestID != "" || second.Code != ErrorCodeInvalidRequest || second.Success || len(second.Results) != 0 {
		t.Fatalf("second result = %#v", second)
	}
}

func TestEnginePublishesQueueFullWithoutExecutingRequest(t *testing.T) {
	t.Parallel()

	transport := newFakeWritebackTransport()
	engine := NewEngine(nil, transport, testWritebackConfig(), zap.NewNop())
	for index := 0; index < requestQueueCapacity; index++ {
		if err := engine.executor.TrySubmit(Request{RequestID: "queued"}); err != nil {
			t.Fatalf("fill queue at %d: %v", index, err)
		}
	}

	engine.handleMessage([]byte(`{"request_id":"req-full","items":[{"point_id":"motor.speed","value":1}]}`))
	result := decodePublishedResult(t, transport.awaitPublished(t))
	if result.RequestID != "req-full" || result.Code != ErrorCodeWriteQueueFull || result.Success || len(result.Results) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestEnginePublishesOneOrderedAggregateResult(t *testing.T) {
	t.Parallel()

	transport := newFakeWritebackTransport()
	engine := NewEngine(nil, transport, testWritebackConfig(), zap.NewNop())
	resolver := newFakeTargetResolver()
	resolver.add("success", &fakeExecutionTarget{pointID: "success", dataType: "Boolean"})
	resolver.add("failure", &fakeExecutionTarget{pointID: "failure", dataType: "Boolean", writeErr: errors.New("write failed")})
	engine.executor = NewQueueExecutor(resolver, engine.publishResult)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	transport.deliver([]byte(`{
		"request_id":"req-aggregate",
		"items":[
			{"point_id":"success","value":true},
			{"point_id":"failure","value":false}
		]
	}`))

	result := decodePublishedResult(t, transport.awaitPublished(t))
	if result.RequestID != "req-aggregate" || result.Success || len(result.Results) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.Results[0].PointID != "success" || !result.Results[0].Success {
		t.Fatalf("first item = %#v", result.Results[0])
	}
	if result.Results[1].PointID != "failure" || result.Results[1].Code != ErrorCodeWriteFailed {
		t.Fatalf("second item = %#v", result.Results[1])
	}
	select {
	case extra := <-transport.published:
		t.Fatalf("request published more than one result: %s", extra)
	default:
	}
	engine.Stop()
}

func TestEngineExecutesLegacyArrayThroughUnifiedPipeline(t *testing.T) {
	t.Parallel()

	transport := newFakeWritebackTransport()
	engine := NewEngine(nil, transport, testWritebackConfig(), zap.NewNop())
	target := &fakeExecutionTarget{pointID: "t1.d1.t1", dataType: "Int32"}
	resolver := newFakeTargetResolver()
	resolver.add("t1.d1.t1", target)
	engine.executor = NewQueueExecutor(resolver, engine.publishResult)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	transport.deliver([]byte(`[{"id":"t1.d1.t1","v":1}]`))

	result := decodePublishedResult(t, transport.awaitPublished(t))
	if !strings.HasPrefix(result.RequestID, "legacy-") || !result.Success || len(result.Results) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Results[0].PointID != "t1.d1.t1" || !result.Results[0].Success {
		t.Fatalf("item result = %#v", result.Results[0])
	}
	if target.writeCountValue() != 1 {
		t.Fatalf("write count = %d, want 1", target.writeCountValue())
	}
	engine.Stop()
}

func TestEngineStopFinishesCurrentAndDropsQueuedRequests(t *testing.T) {
	t.Parallel()

	transport := newFakeWritebackTransport()
	engine := NewEngine(nil, transport, testWritebackConfig(), zap.NewNop())
	engine.shutdownGrace = time.Second
	current := &fakeExecutionTarget{pointID: "current", dataType: "Boolean", writeDelay: 40 * time.Millisecond}
	queued := &fakeExecutionTarget{pointID: "queued", dataType: "Boolean"}
	resolver := newFakeTargetResolver()
	resolver.add("current", current)
	resolver.add("queued", queued)
	engine.executor = NewQueueExecutor(resolver, engine.publishResult)
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	transport.deliver([]byte(`{"request_id":"current","items":[{"point_id":"current","value":true}]}`))
	transport.deliver([]byte(`{"request_id":"queued","items":[{"point_id":"queued","value":true}]}`))
	waitForWriteCount(t, current, 1)
	engine.Stop()

	result := decodePublishedResult(t, transport.awaitPublished(t))
	if result.RequestID != "current" || !result.Success {
		t.Fatalf("result = %#v", result)
	}
	if queued.writeCountValue() != 0 {
		t.Fatalf("queued write count = %d, want 0", queued.writeCountValue())
	}
	if !transport.wasUnsubscribed() {
		t.Fatal("subscription was not stopped before engine shutdown completed")
	}
	select {
	case extra := <-transport.published:
		t.Fatalf("queued request unexpectedly published: %s", extra)
	default:
	}
}

func TestShutdownGracePeriodIsFixedAtTenSeconds(t *testing.T) {
	t.Parallel()
	if shutdownGracePeriod != 10*time.Second {
		t.Fatalf("shutdown grace period = %s", shutdownGracePeriod)
	}
}

func testWritebackConfig() *config.WritebackConfig {
	return &config.WritebackConfig{
		WriteSubject:  "opcua/write",
		ResultSubject: "opcua/write/result",
	}
}

func decodePublishedResult(t *testing.T, data []byte) Result {
	t.Helper()
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; data = %s", err, data)
	}
	return result
}

type fakeWritebackTransport struct {
	mu           sync.Mutex
	handler      func([]byte)
	unsubscribed bool
	published    chan []byte
}

func newFakeWritebackTransport() *fakeWritebackTransport {
	return &fakeWritebackTransport{published: make(chan []byte, 10)}
}

func (t *fakeWritebackTransport) Subscribe(_ context.Context, _ string, handler func([]byte)) (Subscription, error) {
	t.mu.Lock()
	t.handler = handler
	t.mu.Unlock()
	return &fakeWritebackSubscription{transport: t}, nil
}

func (t *fakeWritebackTransport) PublishResult(_ context.Context, _ string, data []byte) error {
	t.published <- append([]byte(nil), data...)
	return nil
}

func (t *fakeWritebackTransport) Close() {}

func (t *fakeWritebackTransport) deliver(data []byte) {
	t.mu.Lock()
	handler := t.handler
	t.mu.Unlock()
	if handler != nil {
		handler(data)
	}
}

func (t *fakeWritebackTransport) awaitPublished(test *testing.T) []byte {
	test.Helper()
	select {
	case data := <-t.published:
		return data
	case <-time.After(time.Second):
		test.Fatal("timed out waiting for published result")
		return nil
	}
}

func (t *fakeWritebackTransport) wasUnsubscribed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.unsubscribed
}

type fakeWritebackSubscription struct {
	transport *fakeWritebackTransport
}

func (s *fakeWritebackSubscription) Unsubscribe() error {
	s.transport.mu.Lock()
	s.transport.unsubscribed = true
	s.transport.handler = nil
	s.transport.mu.Unlock()
	return nil
}
