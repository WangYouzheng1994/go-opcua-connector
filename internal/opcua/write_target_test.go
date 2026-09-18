package opcua

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/config"

	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

func TestResolveWriteTargetReturnsTypedNotFoundError(t *testing.T) {
	t.Parallel()

	client := NewClient(&config.OPCUAConfig{}, zap.NewNop())
	target, err := client.ResolveWriteTarget(collector.PointID("missing.point"))
	if target != nil {
		t.Fatalf("target = %#v, want nil", target)
	}
	if !errors.Is(err, ErrPointNotFound) {
		t.Fatalf("error = %v, want ErrPointNotFound", err)
	}
}

func TestResolvedWriteTargetKeepsSameSourceRef(t *testing.T) {
	t.Parallel()

	originalRef := mustSourceRef(t, "ns=2;s=original")
	replacementRef := mustSourceRef(t, "ns=3;s=replacement")
	backend := &fakeWriteTargetBackend{
		readResponse: dataTypeResponse(t, 6),
		writeResponse: &ua.WriteResponse{
			Results: []ua.StatusCode{ua.StatusOK},
		},
	}
	client := writeTargetClient(backend, map[string]SourceRef{"motor.speed": originalRef})

	target, err := client.ResolveWriteTarget(collector.PointID("motor.speed"))
	if err != nil {
		t.Fatalf("ResolveWriteTarget() error = %v", err)
	}
	client.installPointRegistry(testPointRegistry(map[string]SourceRef{"motor.speed": replacementRef}))

	ctx := context.WithValue(context.Background(), writeTargetContextKey{}, "same-context")
	dataType, err := target.ReadDataType(ctx)
	if err != nil {
		t.Fatalf("ReadDataType() error = %v", err)
	}
	if dataType != "Int32" {
		t.Fatalf("data type = %q, want Int32", dataType)
	}
	if err := target.Write(ctx, int32(1500)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if target.PointID() != collector.PointID("motor.speed") {
		t.Fatalf("PointID() = %q", target.PointID())
	}
	if backend.readContext != ctx || backend.writeContext != ctx {
		t.Fatal("ReadDataType/Write did not pass the caller context to the backend")
	}
	if got := backend.readRequest.NodesToRead[0].NodeID.String(); got != originalRef.String() {
		t.Fatalf("read NodeID = %q, want original %q", got, originalRef.String())
	}
	if got := backend.writeRequest.NodesToWrite[0].NodeID.String(); got != originalRef.String() {
		t.Fatalf("write NodeID = %q, want original %q", got, originalRef.String())
	}
}

func TestWriteTargetPassesCancellationToBackend(t *testing.T) {
	t.Parallel()

	sourceRef := mustSourceRef(t, "ns=2;s=cancelled")
	backend := &fakeWriteTargetBackend{waitForCancellation: true}
	client := writeTargetClient(backend, map[string]SourceRef{"cancelled.point": sourceRef})
	target, err := client.ResolveWriteTarget(collector.PointID("cancelled.point"))
	if err != nil {
		t.Fatalf("ResolveWriteTarget() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := target.ReadDataType(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDataType() error = %v, want context.Canceled", err)
	}
	if err := target.Write(ctx, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write() error = %v, want context.Canceled", err)
	}
}

func TestWriteTargetExposesBadStatusAsOptionalDiagnostic(t *testing.T) {
	t.Parallel()

	backend := &fakeWriteTargetBackend{writeResponse: &ua.WriteResponse{
		Results: []ua.StatusCode{ua.StatusBadNotWritable},
	}}
	client := writeTargetClient(backend, map[string]SourceRef{
		"readonly.point": mustSourceRef(t, "ns=2;s=readonly"),
	})
	target, err := client.ResolveWriteTarget("readonly.point")
	if err != nil {
		t.Fatalf("ResolveWriteTarget() error = %v", err)
	}

	err = target.Write(context.Background(), true)
	if err == nil {
		t.Fatal("Write() error = nil")
	}
	status, ok := WriteStatusOf(err)
	if !ok || status != "BadNotWritable" {
		t.Fatalf("WriteStatusOf() = %q, %v", status, ok)
	}
}

func TestWriteTargetInterfaceDoesNotExposeProtocolIdentity(t *testing.T) {
	t.Parallel()

	typeOfTarget := reflect.TypeOf((*WriteTarget)(nil)).Elem()
	wantMethods := map[string]struct{}{
		"PointID":      {},
		"ReadDataType": {},
		"Write":        {},
	}
	if typeOfTarget.NumMethod() != len(wantMethods) {
		t.Fatalf("WriteTarget method count = %d, want %d", typeOfTarget.NumMethod(), len(wantMethods))
	}
	for index := 0; index < typeOfTarget.NumMethod(); index++ {
		method := typeOfTarget.Method(index)
		if _, ok := wantMethods[method.Name]; !ok {
			t.Fatalf("WriteTarget exposes unexpected method %q", method.Name)
		}
	}
}

func TestWriteTargetFormattingDoesNotExposeSourceRef(t *testing.T) {
	t.Parallel()

	sourceRef := mustSourceRef(t, "ns=2;s=secret-target")
	client := writeTargetClient(&fakeWriteTargetBackend{}, map[string]SourceRef{"public.point": sourceRef})
	target, err := client.ResolveWriteTarget(collector.PointID("public.point"))
	if err != nil {
		t.Fatalf("ResolveWriteTarget() error = %v", err)
	}
	for _, formatted := range []string{fmt.Sprintf("%v", target), fmt.Sprintf("%+v", target), fmt.Sprintf("%#v", target)} {
		if strings.Contains(formatted, sourceRef.String()) || strings.Contains(formatted, "secret-target") {
			t.Fatalf("formatted target leaks SourceRef: %q", formatted)
		}
	}
}

func TestReadDataTypeDoesNotExposeCustomTypeNodeID(t *testing.T) {
	t.Parallel()

	sourceRef := mustSourceRef(t, "ns=2;s=target")
	variant, err := ua.NewVariant(ua.NewStringNodeID(2, "secret-custom-type"))
	if err != nil {
		t.Fatalf("ua.NewVariant() error = %v", err)
	}
	backend := &fakeWriteTargetBackend{readResponse: &ua.ReadResponse{
		Results: []*ua.DataValue{{Status: ua.StatusOK, Value: variant}},
	}}
	client := writeTargetClient(backend, map[string]SourceRef{"custom.point": sourceRef})
	target, err := client.ResolveWriteTarget(collector.PointID("custom.point"))
	if err != nil {
		t.Fatalf("ResolveWriteTarget() error = %v", err)
	}

	_, err = target.ReadDataType(context.Background())
	if err == nil {
		t.Fatal("ReadDataType() error = nil")
	}
	if strings.Contains(err.Error(), "secret-custom-type") || strings.Contains(err.Error(), "ns=2") {
		t.Fatalf("ReadDataType() error leaks custom type NodeID: %v", err)
	}
}

type writeTargetContextKey struct{}

type fakeWriteTargetBackend struct {
	readContext         context.Context
	writeContext        context.Context
	readRequest         *ua.ReadRequest
	writeRequest        *ua.WriteRequest
	readResponse        *ua.ReadResponse
	writeResponse       *ua.WriteResponse
	waitForCancellation bool
}

func (f *fakeWriteTargetBackend) Read(ctx context.Context, request *ua.ReadRequest) (*ua.ReadResponse, error) {
	f.readContext = ctx
	f.readRequest = request
	if f.waitForCancellation {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.readResponse, nil
}

func (f *fakeWriteTargetBackend) Write(ctx context.Context, request *ua.WriteRequest) (*ua.WriteResponse, error) {
	f.writeContext = ctx
	f.writeRequest = request
	if f.waitForCancellation {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.writeResponse, nil
}

func writeTargetClient(backend writeTargetBackend, sources map[string]SourceRef) *Client {
	client := NewClient(&config.OPCUAConfig{}, zap.NewNop())
	client.writeTargetBackend = backend
	client.installPointRegistry(testPointRegistry(sources))
	return client
}

func testPointRegistry(sources map[string]SourceRef) pointRegistry {
	registry := pointRegistry{
		sourceByPointID:     make(map[string]SourceRef, len(sources)),
		pointIDBySource:     make(map[string]string, len(sources)),
		browsePathByPointID: make(map[string][]string, len(sources)),
	}
	for pointID, sourceRef := range sources {
		registry.sourceByPointID[pointID] = sourceRef
		registry.pointIDBySource[sourceRef.String()] = pointID
	}
	return registry
}

func dataTypeResponse(t *testing.T, typeID uint32) *ua.ReadResponse {
	t.Helper()
	variant, err := ua.NewVariant(ua.NewNumericNodeID(0, typeID))
	if err != nil {
		t.Fatalf("ua.NewVariant() error = %v", err)
	}
	return &ua.ReadResponse{
		Results: []*ua.DataValue{{
			Status: ua.StatusOK,
			Value:  variant,
		}},
	}
}
