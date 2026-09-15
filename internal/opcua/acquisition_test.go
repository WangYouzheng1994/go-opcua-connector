package opcua

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"go-opcua-connector/internal/collector"

	goopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

func TestSubscribePointsChecksEveryMonitorResult(t *testing.T) {
	t.Parallel()

	backend := &fakeAcquisitionBackend{subscriptions: []*fakeMonitoredSubscription{{
		response: &ua.CreateMonitoredItemsResponse{Results: []*ua.MonitoredItemCreateResult{
			{StatusCode: ua.StatusOK, MonitoredItemID: 10},
			{StatusCode: ua.StatusBadNodeIDUnknown},
		}},
	}}}
	client, points, tokens := task3Client(t, backend, "point.c", "point.a", "point.b")
	result := client.SubscribePoints(context.Background(), points, tokens, func(collector.PointSample) {})

	if !reflect.DeepEqual(result.Succeeded, []collector.PointID{"point.a"}) {
		t.Fatalf("unexpected successful points: %#v", result.Succeeded)
	}
	wantFailures := []collector.PointFailure{
		{PointID: "point.b", Reason: "monitored item rejected"},
		{PointID: "point.c", Reason: "monitored item response missing"},
	}
	if !reflect.DeepEqual(result.Failed, wantFailures) {
		t.Fatalf("unexpected failed points: %#v", result.Failed)
	}
	if result.Subscription == nil {
		t.Fatal("successful monitored item must retain its subscription")
	}

	subscription := backend.subscriptions[0]
	if subscription.timestamps != ua.TimestampsToReturnBoth || len(subscription.requests) != 3 {
		t.Fatalf("unexpected Monitor call: timestamps=%v requests=%d", subscription.timestamps, len(subscription.requests))
	}
	for index, request := range subscription.requests {
		parameters := request.RequestedParameters
		if parameters.ClientHandle != uint32(index+1) || parameters.QueueSize != 1 || !parameters.DiscardOldest || parameters.SamplingInterval != 0 {
			t.Fatalf("unexpected monitored item parameters at %d: %#v", index, parameters)
		}
	}
	if backend.params[0].Interval != 100*time.Millisecond {
		t.Fatalf("publishing interval = %s", backend.params[0].Interval)
	}
	if err := result.Subscription.Close(context.Background()); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
	if subscription.cancelCount() != 1 {
		t.Fatalf("subscription cancel count = %d", subscription.cancelCount())
	}
}

func TestSubscribePointsBatchesAndAllocatesUniqueClientHandles(t *testing.T) {
	t.Parallel()

	backend := &fakeAcquisitionBackend{}
	pointNames := make([]string, monitoredItemsPerSubscription+1)
	for index := range pointNames {
		pointNames[index] = fmt.Sprintf("point.%04d", index)
	}
	client, points, tokens := task3Client(t, backend, pointNames...)
	result := client.SubscribePoints(context.Background(), points, tokens, func(collector.PointSample) {})
	if len(result.Failed) != 0 || len(result.Succeeded) != len(points) {
		t.Fatalf("unexpected batched subscription result: succeeded=%d failed=%#v", len(result.Succeeded), result.Failed)
	}
	if len(backend.subscriptions) != 2 || len(backend.subscriptions[0].requests) != monitoredItemsPerSubscription || len(backend.subscriptions[1].requests) != 1 {
		t.Fatalf("unexpected subscription batches: %d", len(backend.subscriptions))
	}
	handles := make(map[uint32]struct{}, len(points))
	for _, subscription := range backend.subscriptions {
		for _, request := range subscription.requests {
			handle := request.RequestedParameters.ClientHandle
			if _, exists := handles[handle]; exists {
				t.Fatalf("duplicate ClientHandle %d", handle)
			}
			handles[handle] = struct{}{}
		}
	}
	if err := result.Subscription.Close(context.Background()); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
}

func TestSubscribePointsCancelsSubscriptionWhenMonitorRequestFails(t *testing.T) {
	t.Parallel()

	subscription := &fakeMonitoredSubscription{monitorErr: errors.New("monitor unavailable")}
	backend := &fakeAcquisitionBackend{subscriptions: []*fakeMonitoredSubscription{subscription}}
	client, points, tokens := task3Client(t, backend, "point.a", "point.b")
	result := client.SubscribePoints(context.Background(), points, tokens, func(collector.PointSample) {})

	if result.Subscription != nil || len(result.Succeeded) != 0 || !reflect.DeepEqual(result.Failed, []collector.PointFailure{
		{PointID: "point.a", Reason: "monitored item creation failed"},
		{PointID: "point.b", Reason: "monitored item creation failed"},
	}) {
		t.Fatalf("unexpected monitor failure result: %#v", result)
	}
	if subscription.cancelCount() != 1 {
		t.Fatalf("failed subscription cancel count = %d", subscription.cancelCount())
	}
}

func TestSubscribePointsNormalizesNotificationAndIgnoresUnknownHandle(t *testing.T) {
	t.Parallel()

	receivedAt := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	sourceAt := receivedAt.Add(-2 * time.Second)
	serverAt := receivedAt.Add(-time.Second)
	backend := &fakeAcquisitionBackend{}
	client, points, tokens := task3Client(t, backend, "point.a")
	client.adapterNow = func() time.Time { return receivedAt }
	samples := make(chan collector.PointSample, 2)
	result := client.SubscribePoints(context.Background(), points, tokens, func(sample collector.PointSample) {
		samples <- sample
	})
	if result.Subscription == nil || len(result.Failed) != 0 {
		t.Fatalf("subscription failed: %#v", result)
	}

	valueBytes := []byte{1, 2, 3}
	backend.notificationChannels[0] <- &goopcua.PublishNotificationData{Value: &ua.DataChangeNotification{
		MonitoredItems: []*ua.MonitoredItemNotification{
			{ClientHandle: 999, Value: dataValue(t, ua.StatusOK, "ignored", sourceAt, serverAt)},
			{ClientHandle: 1, Value: dataValue(t, ua.StatusOK, valueBytes, sourceAt, serverAt)},
		},
	}}

	select {
	case sample := <-samples:
		if sample.PointID != "point.a" || sample.Generation != tokens["point.a"] || sample.Validity != collector.SampleValid || !sample.HasValue {
			t.Fatalf("unexpected normalized sample: %#v", sample)
		}
		if !sample.ObservedAt.Equal(sourceAt) || !sample.ReceivedAt.Equal(receivedAt) {
			t.Fatalf("unexpected sample timestamps: %#v", sample)
		}
		valueBytes[0] = 9
		if !reflect.DeepEqual(sample.Value, []byte{1, 2, 3}) {
			t.Fatalf("sample value was not copied: %#v", sample.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for normalized sample")
	}
	if err := result.Subscription.Close(context.Background()); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
}

func TestPointSubscriptionCloseWaitsForInFlightHandler(t *testing.T) {
	t.Parallel()

	subscription := &fakeMonitoredSubscription{cancelled: make(chan struct{})}
	notifications := make(chan *goopcua.PublishNotificationData, 1)
	managed := newManagedPointSubscription(context.Background())
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	managed.add(
		subscription,
		notifications,
		map[uint32]monitoredItemBinding{1: {
			point: monitoredPoint{
				pointID:    "point.a",
				generation: collector.GenerationToken{Connection: 1, Monitor: 1},
			},
			monitoredItemID: 10,
		}},
		time.Now,
		nil,
		func(collector.PointSample) {
			close(handlerStarted)
			<-releaseHandler
		},
	)
	value := dataValue(t, ua.StatusOK, "value", time.Now(), time.Time{})
	notifications <- &goopcua.PublishNotificationData{Value: &ua.DataChangeNotification{
		MonitoredItems: []*ua.MonitoredItemNotification{{ClientHandle: 1, Value: value}},
	}}
	<-handlerStarted

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- managed.Close(context.Background())
	}()
	<-subscription.cancelled
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before in-flight handler completed: %v", err)
	default:
	}
	close(releaseHandler)
	if err := <-closeResult; err != nil {
		t.Fatalf("close subscription: %v", err)
	}
}

func TestNormalizePointSampleUsesTimestampFallbackAndRejectsBadOrEmptyValue(t *testing.T) {
	t.Parallel()

	receivedAt := time.Date(2026, 9, 11, 11, 0, 0, 0, time.UTC)
	serverAt := receivedAt.Add(-time.Second)
	point := monitoredPoint{pointID: "point.a", generation: collector.GenerationToken{Connection: 1, Monitor: 2}}

	bad := normalizePointSample(point, dataValue(t, ua.StatusBadNodeIDUnknown, int32(7), time.Time{}, serverAt), receivedAt)
	if bad.Validity != collector.SampleInvalid || !bad.HasValue || !bad.ObservedAt.Equal(serverAt) {
		t.Fatalf("unexpected bad sample: %#v", bad)
	}
	empty := normalizePointSample(point, &ua.DataValue{Status: ua.StatusOK}, receivedAt)
	if empty.Validity != collector.SampleInvalid || empty.HasValue || !empty.ObservedAt.Equal(receivedAt) {
		t.Fatalf("unexpected empty sample: %#v", empty)
	}
}

func TestReadInitialConvertsPartialResponseWithoutLosingSuccessfulPoints(t *testing.T) {
	t.Parallel()

	checkedAt := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	sourceAt := checkedAt.Add(-2 * time.Second)
	serverAt := checkedAt.Add(-time.Second)
	readBytes := []byte{4, 5, 6}
	backend := &fakeAcquisitionBackend{readResponse: &ua.ReadResponse{Results: []*ua.DataValue{
		dataValue(t, ua.StatusOK, readBytes, sourceAt, serverAt),
		dataValue(t, ua.StatusBadNodeIDUnknown, false, time.Time{}, serverAt),
	}}}
	client, points, tokens := task3Client(t, backend, "point.c", "point.b", "point.a")
	client.adapterNow = func() time.Time { return checkedAt }
	result := client.ReadInitial(context.Background(), points, tokens)

	if len(result.Verifications) != 2 {
		t.Fatalf("verification count = %d", len(result.Verifications))
	}
	valid := result.Verifications[0]
	if valid.PointID != "point.a" || valid.Validity != collector.SampleValid || !valid.HasValue ||
		!valid.ObservedAt.Equal(sourceAt) || !valid.CheckedAt.Equal(checkedAt) || valid.Generation != tokens["point.a"] {
		t.Fatalf("unexpected valid verification: %#v", valid)
	}
	readBytes[0] = 9
	if !reflect.DeepEqual(valid.Value, []byte{4, 5, 6}) {
		t.Fatalf("verification value was not copied: %#v", valid.Value)
	}
	invalid := result.Verifications[1]
	if invalid.PointID != "point.b" || invalid.Validity != collector.SampleInvalid || !invalid.HasValue || !invalid.ObservedAt.Equal(serverAt) {
		t.Fatalf("unexpected invalid verification: %#v", invalid)
	}
	if !reflect.DeepEqual(result.Failed, []collector.PointFailure{{PointID: "point.c", Reason: "read response missing"}}) {
		t.Fatalf("unexpected read failures: %#v", result.Failed)
	}
	if backend.readRequest == nil || backend.readRequest.TimestampsToReturn != ua.TimestampsToReturnBoth || len(backend.readRequest.NodesToRead) != 3 {
		t.Fatalf("unexpected read request: %#v", backend.readRequest)
	}
}

func TestReadInitialKeepsWholeRequestFailureSeparateFromPoints(t *testing.T) {
	t.Parallel()

	backend := &fakeAcquisitionBackend{readErr: errors.New("transport unavailable")}
	client, points, tokens := task3Client(t, backend, "point.b", "point.a")
	result := client.ReadInitial(context.Background(), points, tokens)
	if len(result.Verifications) != 0 || len(result.Failed) != 0 || result.RequestFailure != "read request failed" {
		t.Fatalf("unexpected whole-read failure: %#v", result)
	}
}

func TestPointSubscriptionRemoveStopsRoutingRemovedHandle(t *testing.T) {
	t.Parallel()

	backend := &fakeAcquisitionBackend{}
	client, points, tokens := task3Client(t, backend, "point.a")
	samples := make(chan collector.PointSample, 1)
	result := client.SubscribePoints(context.Background(), points, tokens, func(sample collector.PointSample) {
		samples <- sample
	})
	if result.Subscription == nil || len(result.Succeeded) != 1 {
		t.Fatalf("subscription result = %#v", result)
	}
	if err := result.Subscription.Remove(context.Background(), "point.a"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	backend.notificationChannels[0] <- &goopcua.PublishNotificationData{Value: &ua.DataChangeNotification{
		MonitoredItems: []*ua.MonitoredItemNotification{{ClientHandle: 1, Value: dataValue(t, ua.StatusOK, int32(1), time.Now(), time.Time{})}},
	}}
	select {
	case sample := <-samples:
		t.Fatalf("removed handle still routed a sample: %#v", sample)
	case <-time.After(50 * time.Millisecond):
	}
	backend.subscriptions[0].mu.Lock()
	unmonitored := append([]uint32(nil), backend.subscriptions[0].unmonitored...)
	backend.subscriptions[0].mu.Unlock()
	if !reflect.DeepEqual(unmonitored, []uint32{1}) {
		t.Fatalf("unmonitored IDs = %#v", unmonitored)
	}
	managed := result.Subscription.(*managedPointSubscription)
	managed.mu.Lock()
	_, registered := managed.registrations["point.a"]
	managed.mu.Unlock()
	if registered {
		t.Fatal("successful unmonitor kept local registration")
	}
	if err := result.Subscription.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestPointSubscriptionRemoveFailureKeepsRegistrationForClose(t *testing.T) {
	t.Parallel()

	protocolSubscription := &fakeMonitoredSubscription{unmonitorErr: errors.New("delete failed")}
	backend := &fakeAcquisitionBackend{subscriptions: []*fakeMonitoredSubscription{protocolSubscription}}
	client, points, tokens := task3Client(t, backend, "point.a")
	samples := make(chan collector.PointSample, 1)
	result := client.SubscribePoints(context.Background(), points, tokens, func(sample collector.PointSample) {
		samples <- sample
	})
	if err := result.Subscription.Remove(context.Background(), "point.a"); err == nil {
		t.Fatal("expected Remove() failure")
	}
	managed := result.Subscription.(*managedPointSubscription)
	managed.mu.Lock()
	_, registered := managed.registrations["point.a"]
	managed.mu.Unlock()
	if !registered {
		t.Fatal("failed unmonitor removed local registration")
	}
	backend.notificationChannels[0] <- &goopcua.PublishNotificationData{Value: &ua.DataChangeNotification{
		MonitoredItems: []*ua.MonitoredItemNotification{{ClientHandle: 1, Value: dataValue(t, ua.StatusOK, int32(1), time.Now(), time.Time{})}},
	}}
	select {
	case <-samples:
	case <-time.After(time.Second):
		t.Fatal("failed unmonitor removed notification routing")
	}
	if err := result.Subscription.Close(context.Background()); err != nil {
		t.Fatalf("Close() after failed Remove() error = %v", err)
	}
	if protocolSubscription.cancelCount() != 1 {
		t.Fatalf("underlying cancel count = %d", protocolSubscription.cancelCount())
	}
}

func TestPointSubscriptionRemoveAndCloseAreSerialized(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	protocolSubscription := &fakeMonitoredSubscription{unmonitorStarted: started, unmonitorRelease: release}
	backend := &fakeAcquisitionBackend{subscriptions: []*fakeMonitoredSubscription{protocolSubscription}}
	client, points, tokens := task3Client(t, backend, "point.a")
	result := client.SubscribePoints(context.Background(), points, tokens, func(collector.PointSample) {})
	removeDone := make(chan error, 1)
	go func() { removeDone <- result.Subscription.Remove(context.Background(), "point.a") }()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- result.Subscription.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close() bypassed in-flight Remove(): %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-removeDone; err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if protocolSubscription.cancelCount() != 1 {
		t.Fatalf("underlying cancel count = %d", protocolSubscription.cancelCount())
	}
}

func task3Client(
	t *testing.T,
	backend acquisitionBackend,
	pointNames ...string,
) (*Client, []collector.PointID, map[collector.PointID]collector.GenerationToken) {
	t.Helper()
	client := &Client{
		adapterBackend:      backend,
		sourceByPointID:     make(map[string]SourceRef, len(pointNames)),
		pointIDBySource:     make(map[string]string, len(pointNames)),
		browsePathByPointID: make(map[string][]string, len(pointNames)),
		nextClientHandle:    1,
		adapterNow:          time.Now,
	}
	points := make([]collector.PointID, 0, len(pointNames))
	tokens := make(map[collector.PointID]collector.GenerationToken, len(pointNames))
	for index, name := range pointNames {
		pointID := collector.PointID(name)
		sourceRef := mustSourceRef(t, fmt.Sprintf("ns=2;s=source-%04d", index))
		client.sourceByPointID[name] = sourceRef
		client.pointIDBySource[sourceRef.String()] = name
		client.browsePathByPointID[name] = []string{"Objects", name}
		points = append(points, pointID)
		tokens[pointID] = collector.GenerationToken{Connection: 1, Monitor: uint64(index + 1)}
	}
	return client, points, tokens
}

func dataValue(
	t *testing.T,
	status ua.StatusCode,
	value any,
	sourceTimestamp time.Time,
	serverTimestamp time.Time,
) *ua.DataValue {
	t.Helper()
	variant, err := ua.NewVariant(value)
	if err != nil {
		t.Fatalf("new variant: %v", err)
	}
	return &ua.DataValue{
		Value:           variant,
		Status:          status,
		SourceTimestamp: sourceTimestamp,
		ServerTimestamp: serverTimestamp,
	}
}

type fakeAcquisitionBackend struct {
	subscriptions        []*fakeMonitoredSubscription
	params               []*goopcua.SubscriptionParameters
	notificationChannels []chan *goopcua.PublishNotificationData
	subscribeErr         error
	readResponse         *ua.ReadResponse
	readErr              error
	readRequest          *ua.ReadRequest
}

func (f *fakeAcquisitionBackend) subscribe(
	_ context.Context,
	params *goopcua.SubscriptionParameters,
	notifications chan *goopcua.PublishNotificationData,
) (monitoredSubscription, error) {
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	f.params = append(f.params, params)
	f.notificationChannels = append(f.notificationChannels, notifications)
	index := len(f.params) - 1
	if index >= len(f.subscriptions) {
		f.subscriptions = append(f.subscriptions, &fakeMonitoredSubscription{})
	}
	return f.subscriptions[index], nil
}

func (f *fakeAcquisitionBackend) read(_ context.Context, request *ua.ReadRequest) (*ua.ReadResponse, error) {
	f.readRequest = request
	return f.readResponse, f.readErr
}

type fakeMonitoredSubscription struct {
	response          *ua.CreateMonitoredItemsResponse
	monitorErr        error
	timestamps        ua.TimestampsToReturn
	requests          []*ua.MonitoredItemCreateRequest
	mu                sync.Mutex
	cancels           int
	cancelled         chan struct{}
	cancelOnce        sync.Once
	unmonitored       []uint32
	unmonitorResponse *ua.DeleteMonitoredItemsResponse
	unmonitorErr      error
	unmonitorStarted  chan struct{}
	unmonitorRelease  <-chan struct{}
	unmonitorOnce     sync.Once
}

func (f *fakeMonitoredSubscription) unmonitor(_ context.Context, monitoredItemIDs ...uint32) (*ua.DeleteMonitoredItemsResponse, error) {
	f.mu.Lock()
	f.unmonitored = append(f.unmonitored, monitoredItemIDs...)
	started := f.unmonitorStarted
	release := f.unmonitorRelease
	f.mu.Unlock()
	if started != nil {
		f.unmonitorOnce.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}
	if f.unmonitorErr != nil {
		return nil, f.unmonitorErr
	}
	if f.unmonitorResponse != nil {
		return f.unmonitorResponse, nil
	}
	results := make([]ua.StatusCode, len(monitoredItemIDs))
	return &ua.DeleteMonitoredItemsResponse{Results: results}, nil
}

func (f *fakeMonitoredSubscription) monitor(
	_ context.Context,
	timestamps ua.TimestampsToReturn,
	items ...*ua.MonitoredItemCreateRequest,
) (*ua.CreateMonitoredItemsResponse, error) {
	f.timestamps = timestamps
	f.requests = append([]*ua.MonitoredItemCreateRequest(nil), items...)
	if f.monitorErr != nil {
		return nil, f.monitorErr
	}
	if f.response != nil {
		return f.response, nil
	}
	results := make([]*ua.MonitoredItemCreateResult, len(items))
	for index := range results {
		results[index] = &ua.MonitoredItemCreateResult{StatusCode: ua.StatusOK, MonitoredItemID: uint32(index + 1)}
	}
	return &ua.CreateMonitoredItemsResponse{Results: results}, nil
}

func (f *fakeMonitoredSubscription) cancel(context.Context) error {
	f.mu.Lock()
	f.cancels++
	f.mu.Unlock()
	if f.cancelled != nil {
		f.cancelOnce.Do(func() { close(f.cancelled) })
	}
	return nil
}

func (f *fakeMonitoredSubscription) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancels
}
