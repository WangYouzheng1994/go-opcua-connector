package writeback

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go-opcua-connector/internal/collector"
)

func TestDecodeRequestPreservesEnvelopeAndNumberPrecision(t *testing.T) {
	t.Parallel()

	request, err := DecodeRequest([]byte(`{
		"request_id":"req-001",
		"items":[{"point_id":"motor.speed","value":9007199254740993}]
	}`))
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	if request.RequestID != "req-001" {
		t.Fatalf("RequestID = %q", request.RequestID)
	}
	if len(request.Items) != 1 || request.Items[0].PointID != collector.PointID("motor.speed") {
		t.Fatalf("Items = %#v", request.Items)
	}
	number, ok := request.Items[0].Value.(json.Number)
	if !ok || number.String() != "9007199254740993" {
		t.Fatalf("Value = %#v (%T)", request.Items[0].Value, request.Items[0].Value)
	}
}

func TestDecodeRequestAcceptsItemLimits(t *testing.T) {
	t.Parallel()

	for _, count := range []int{1, MaxRequestItems} {
		count := count
		t.Run(fmt.Sprintf("items_%d", count), func(t *testing.T) {
			t.Parallel()
			request, err := DecodeRequest(requestPayload(t, "req-limit", count))
			if err != nil {
				t.Fatalf("DecodeRequest() error = %v", err)
			}
			if len(request.Items) != count {
				t.Fatalf("items length = %d, want %d", len(request.Items), count)
			}
		})
	}
}

func TestDecodeRequestRejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{name: "empty payload", payload: ``, want: "invalid JSON"},
		{name: "empty request id", payload: `{"request_id":" ","items":[{"point_id":"A","value":1}]}`, want: "request_id is required"},
		{name: "missing items", payload: `{"request_id":"req"}`, want: "items must contain at least one item"},
		{name: "empty items", payload: `{"request_id":"req","items":[]}`, want: "items must contain at least one item"},
		{name: "empty point id", payload: `{"request_id":"req","items":[{"point_id":" ","value":1}]}`, want: "items[0].point_id is required"},
		{name: "missing value", payload: `{"request_id":"req","items":[{"point_id":"A"}]}`, want: "items[0].value is required"},
		{name: "null value", payload: `{"request_id":"req","items":[{"point_id":"A","value":null}]}`, want: "items[0].value must not be null"},
		{name: "unknown envelope field", payload: `{"request_id":"req","items":[{"point_id":"A","value":1}],"extra":true}`, want: "unknown field"},
		{name: "legacy single write payload", payload: `{"request_id":"req","node_id":"ns=2;s=A","value":"1","value_type":"Int32"}`, want: "unknown field"},
		{name: "legacy batch write payload", payload: `[{"id":"A","v":1}]`, want: "cannot unmarshal"},
		{name: "node id is forbidden", payload: `{"request_id":"req","items":[{"point_id":"A","node_id":"ns=2;s=A","value":1}]}`, want: "unknown field"},
		{name: "value type is forbidden", payload: `{"request_id":"req","items":[{"point_id":"A","value":1,"value_type":"Int32"}]}`, want: "unknown field"},
		{name: "trailing JSON", payload: `{"request_id":"req","items":[{"point_id":"A","value":1}]} {}`, want: "multiple JSON values"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeRequest([]byte(test.payload))
			if err == nil {
				t.Fatal("DecodeRequest() error = nil")
			}
			var invalid *InvalidRequestError
			if !errors.As(err, &invalid) {
				t.Fatalf("error type = %T, want *InvalidRequestError", err)
			}
			if invalid.Code() != ErrorCodeInvalidRequest {
				t.Fatalf("error code = %q, want %q", invalid.Code(), ErrorCodeInvalidRequest)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func TestDecodeRequestRejectsMoreThanMaximumItems(t *testing.T) {
	t.Parallel()

	_, err := DecodeRequest(requestPayload(t, "req-too-many", MaxRequestItems+1))
	if err == nil || !strings.Contains(err.Error(), "items exceeds maximum limit of 100") {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
}

func TestDecodeRequestPreservesDuplicatePointIDOrder(t *testing.T) {
	t.Parallel()

	request, err := DecodeRequest([]byte(`{
		"request_id":"duplicate-request-id",
		"items":[
			{"point_id":"motor.start","value":true},
			{"point_id":"motor.start","value":false}
		]
	}`))
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	if len(request.Items) != 2 {
		t.Fatalf("items length = %d", len(request.Items))
	}
	if request.Items[0].PointID != request.Items[1].PointID {
		t.Fatalf("point IDs = %q, %q", request.Items[0].PointID, request.Items[1].PointID)
	}
	if request.Items[0].Value != true || request.Items[1].Value != false {
		t.Fatalf("values = %#v, %#v", request.Items[0].Value, request.Items[1].Value)
	}
}

func TestDecodeRequestDoesNotDeduplicateRequestID(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"request_id":"same-id","items":[{"point_id":"A","value":1}]}`)
	first, firstErr := DecodeRequest(payload)
	second, secondErr := DecodeRequest(payload)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("DecodeRequest() errors = %v, %v", firstErr, secondErr)
	}
	if first.RequestID != "same-id" || second.RequestID != "same-id" {
		t.Fatalf("request IDs = %q, %q", first.RequestID, second.RequestID)
	}
}

func TestResultJSONUsesPointIDAndStableErrorFields(t *testing.T) {
	t.Parallel()

	result := Result{
		RequestID: "req-001",
		Success:   false,
		Results: []ItemResult{
			{PointID: collector.PointID("A"), Success: true},
			{
				PointID:     collector.PointID("B"),
				Success:     false,
				Code:        ErrorCodeWriteFailed,
				Message:     "OPC UA server rejected write",
				OPCUAStatus: "BadNotWritable",
			},
		},
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	jsonText := string(data)
	for _, forbidden := range []string{"node_id", "value_type", "completed_at", "duration"} {
		if strings.Contains(jsonText, forbidden) {
			t.Fatalf("result JSON contains %q: %s", forbidden, jsonText)
		}
	}
	for _, required := range []string{`"request_id":"req-001"`, `"point_id":"B"`, `"code":"WRITE_FAILED"`, `"opcua_status":"BadNotWritable"`} {
		if !strings.Contains(jsonText, required) {
			t.Fatalf("result JSON missing %s: %s", required, jsonText)
		}
	}
}

func TestNewRejectedResultSerializesEmptyResults(t *testing.T) {
	t.Parallel()

	result := NewRejectedResult("req-queue", ErrorCodeWriteQueueFull, "write request queue is full")
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !strings.Contains(string(data), `"results":[]`) {
		t.Fatalf("result JSON = %s", data)
	}
}

func TestRequestIDFromInvalidPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{name: "valid id with invalid items", payload: `{"request_id":"req-invalid","items":[]}`, want: "req-invalid"},
		{name: "unknown field", payload: `{"request_id":"req-unknown","items":[],"extra":true}`, want: "req-unknown"},
		{name: "missing id", payload: `{"items":[]}`},
		{name: "blank id", payload: `{"request_id":" ","items":[]}`},
		{name: "non-string id", payload: `{"request_id":123,"items":[]}`},
		{name: "broken JSON", payload: `{"request_id":"req-broken"`},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := requestIDFromPayload([]byte(test.payload)); got != test.want {
				t.Fatalf("requestIDFromPayload() = %q, want %q", got, test.want)
			}
		})
	}
}

func requestPayload(t *testing.T, requestID string, itemCount int) []byte {
	t.Helper()
	items := make([]map[string]any, itemCount)
	for i := range items {
		items[i] = map[string]any{
			"point_id": fmt.Sprintf("point.%d", i),
			"value":    i,
		}
	}
	payload, err := json.Marshal(map[string]any{
		"request_id": requestID,
		"items":      items,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return payload
}
