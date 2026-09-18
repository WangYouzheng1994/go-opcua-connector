package writeback

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestDecodeIncomingRequestAcceptsLegacyArray(t *testing.T) {
	t.Parallel()

	request, err := decodeIncomingRequest([]byte(`[
		{"id":"t1.d1.t1","v":1},
		{"id":"t1.d1.t1","v":0},
		{"id":"counter","v":18446744073709551615}
	]`))
	if err != nil {
		t.Fatalf("decodeIncomingRequest() error = %v", err)
	}
	if !strings.HasPrefix(request.RequestID, "legacy-") {
		t.Fatalf("RequestID = %q", request.RequestID)
	}
	if len(request.Items) != 3 {
		t.Fatalf("items length = %d", len(request.Items))
	}
	if request.Items[0].PointID != "t1.d1.t1" || request.Items[1].PointID != "t1.d1.t1" {
		t.Fatalf("duplicate point order was not preserved: %#v", request.Items)
	}
	if request.Items[0].Value != json.Number("1") || request.Items[1].Value != json.Number("0") {
		t.Fatalf("legacy values = %#v, %#v", request.Items[0].Value, request.Items[1].Value)
	}
	if request.Items[2].Value != json.Number("18446744073709551615") {
		t.Fatalf("large value = %#v (%T)", request.Items[2].Value, request.Items[2].Value)
	}
}

func TestDecodeIncomingRequestKeepsUnifiedProtocolStrict(t *testing.T) {
	t.Parallel()

	request, err := decodeIncomingRequest([]byte(`{"request_id":"req-001","items":[{"point_id":"A","value":1}]}`))
	if err != nil {
		t.Fatalf("decodeIncomingRequest() error = %v", err)
	}
	if request.RequestID != "req-001" || len(request.Items) != 1 || request.Items[0].PointID != "A" {
		t.Fatalf("request = %#v", request)
	}
	_, err = decodeIncomingRequest([]byte(`{"request_id":"req-legacy-object","id":"A","v":1}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("legacy object error = %v", err)
	}
}

func TestDecodeIncomingRequestRejectsInvalidLegacyArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "empty", payload: []byte(`[]`), want: "at least one"},
		{name: "missing id", payload: []byte(`[{"v":1}]`), want: "id is required"},
		{name: "missing value", payload: []byte(`[{"id":"A"}]`), want: "v is required"},
		{name: "null value", payload: []byte(`[{"id":"A","v":null}]`), want: "must not be null"},
		{name: "unknown field", payload: []byte(`[{"id":"A","v":1,"node_id":"ns=2;s=A"}]`), want: "unknown field"},
		{name: "trailing JSON", payload: []byte(`[{"id":"A","v":1}] {}`), want: "multiple JSON values"},
		{name: "too many", payload: legacyRequestPayload(t, MaxRequestItems+1), want: "maximum limit of 100"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeIncomingRequest(test.payload)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("decodeIncomingRequest() error = %v, want %q", err, test.want)
			}
		})
	}
}

func legacyRequestPayload(t *testing.T, count int) []byte {
	t.Helper()
	items := make([]legacyRequestItem, count)
	for index := range items {
		items[index] = legacyRequestItem{ID: fmt.Sprintf("point.%d", index), Value: json.RawMessage(`1`)}
	}
	data, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}
