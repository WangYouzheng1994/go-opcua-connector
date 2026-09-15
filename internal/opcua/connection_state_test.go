package opcua

import (
	"testing"

	"go-opcua-connector/internal/collector"

	gopcua "github.com/gopcua/opcua"
)

func TestConnectionStateUsesUnderlyingClientState(t *testing.T) {
	t.Parallel()

	underlying, err := gopcua.NewClient("opc.tcp://localhost:4840")
	if err != nil {
		t.Fatalf("gopcua.NewClient() error = %v", err)
	}
	client := &Client{client: underlying}
	if state := client.ConnectionState(); state != gopcua.Closed {
		t.Fatalf("ConnectionState() = %v, want %v", state, gopcua.Closed)
	}
	if client.IsConnected() {
		t.Fatal("non-nil but closed underlying client was reported connected")
	}
	adapter := NewAcquisitionAdapter(client, nil)
	if state := adapter.ConnectionState(); state != collector.SourceDisconnected {
		t.Fatalf("adapter ConnectionState() = %v, want %v", state, collector.SourceDisconnected)
	}
}
