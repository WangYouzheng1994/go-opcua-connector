package model

import "testing"

func TestNewBatchMessagePreservesProtocolIndependentPointID(t *testing.T) {
	t.Parallel()

	message := NewBatchMessage([]DataPoint{{NodeID: "ns=2;s=business.point", Value: 0, Quality: "Good"}})
	if len(message.Values) != 1 {
		t.Fatalf("values length = %d", len(message.Values))
	}
	if message.Values[0].ID != "ns=2;s=business.point" {
		t.Fatalf("point ID = %q", message.Values[0].ID)
	}
	if !message.Values[0].Q || message.Values[0].V != 0 {
		t.Fatalf("batch point = %#v", message.Values[0])
	}
}
