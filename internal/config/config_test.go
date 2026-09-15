package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateRejectsEmptyPointIDOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides []OPCUAPointIDOverride
	}{
		{name: "empty source", overrides: []OPCUAPointIDOverride{{SourceRef: " ", PointID: "point"}}},
		{name: "empty point ID", overrides: []OPCUAPointIDOverride{{SourceRef: "ns=2;s=tag", PointID: " "}}},
	}
	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			cfg := validAppConfigForTest()
			cfg.OPCUA.PointIDOverrides = testCase.overrides
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected point ID override validation error")
			}
		})
	}
}

func TestValidateAcceptsPointIDOverride(t *testing.T) {
	t.Parallel()

	cfg := validAppConfigForTest()
	cfg.OPCUA.PointIDOverrides = []OPCUAPointIDOverride{
		{SourceRef: "ns=2;s=Channel.Device.Tag", PointID: "boiler.temperature"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateSetsAcquisitionDefaults(t *testing.T) {
	t.Parallel()

	cfg := validAppConfigForTest()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.Collector.SubscriptionRetryIntervalSec != 30 || cfg.Collector.ReadBatchSize != 500 || cfg.Collector.ReadTimeoutSec != 10 ||
		cfg.Collector.PublishTimeoutMs != 5000 || cfg.Collector.MonitorIntervalSec != 60 || cfg.Collector.SubscriptionTopic != "opcua/data" ||
		cfg.Collector.StaleThresholdSec != 30 {
		t.Fatalf("acquisition defaults = %#v", cfg.Collector)
	}
}

func TestLoaderReadsPointIDOverrides(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte(`
opcua:
  endpoint: opc.tcp://localhost:4840
  point_id_overrides:
    - source_ref: "ns=2;s=Channel.Device.Tag"
      point_id: boiler.temperature
nats:
  urls: nats://localhost:4222
`)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := NewLoader(dir, "config").Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.OPCUA.PointIDOverrides) != 1 {
		t.Fatalf("PointID overrides = %#v", cfg.OPCUA.PointIDOverrides)
	}
	if got := cfg.OPCUA.PointIDOverrides[0]; got.SourceRef != "ns=2;s=Channel.Device.Tag" || got.PointID != "boiler.temperature" {
		t.Fatalf("PointID override = %#v", got)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile(filepath.Join("..", "..", "config.yaml.example"))
	if err != nil {
		t.Fatalf("read example config: %v", err)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), content, 0o600); err != nil {
		t.Fatalf("write example config: %v", err)
	}

	cfg, err := NewLoader(dir, "config").Load()
	if err != nil {
		t.Fatalf("load example config: %v", err)
	}
	if cfg.OPCUA.Endpoint == "" || len(cfg.Collector.SubscriptionNodes) == 0 {
		t.Fatalf("example config misses OPC UA test target: %#v", cfg)
	}
	if cfg.Collector.BatchSize != 100 || cfg.Collector.OutputType != OutputTypeNATS {
		t.Fatalf("example collector config = %#v", cfg.Collector)
	}
}

func validAppConfigForTest() *AppConfig {
	return &AppConfig{
		OPCUA: OPCUAConfig{Endpoint: "opc.tcp://localhost:4840"},
		NATS:  NATSConfig{URLs: "nats://localhost:4222"},
	}
}
