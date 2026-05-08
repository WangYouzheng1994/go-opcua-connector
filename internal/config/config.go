package config

import "fmt"

/**
 * OPC UA服务器配置
 *
 * @author 王有政
 */
type OPCUAConfig struct {
	Endpoint       string `mapstructure:"endpoint"`
	Certificate    string `mapstructure:"certificate"`
	PrivateKey     string `mapstructure:"private_key"`
	Username       string `mapstructure:"username"`
	Password       string `mapstructure:"password"`
	SecurityPolicy string `mapstructure:"security_policy"`
	SecurityMode   string `mapstructure:"security_mode"`
	ConnectTimeout int    `mapstructure:"connect_timeout"`
	RequestTimeout int    `mapstructure:"request_timeout"`
}

/**
 * NATS配置
 *
 * @author 王有政
 */
type NATSConfig struct {
	URLs            string `mapstructure:"urls"`
	Username        string `mapstructure:"username"`
	Password        string `mapstructure:"password"`
	MaxReconnects   int    `mapstructure:"max_reconnects"`
	ReconnectWaitMs int    `mapstructure:"reconnect_wait_ms"`
}

/**
 * 采集器配置
 *
 * @author 王有政
 */
type CollectorConfig struct {
	WorkerCount           int      `mapstructure:"worker_count"`
	BatchSize             int      `mapstructure:"batch_size"`
	ChannelBufferSize     int      `mapstructure:"channel_buffer_size"`
	PublishTimeoutMs      int      `mapstructure:"publish_timeout_ms"`
	SubscriptionNodes     []string `mapstructure:"subscription_nodes"`
	SubscriptionTopic     string   `mapstructure:"subscription_topic"`
	MonitorIntervalSec    int      `mapstructure:"monitor_interval_sec"`
	StaleThresholdSec     int      `mapstructure:"stale_threshold_sec"`
	HeartbeatIntervalSec  int      `mapstructure:"heartbeat_interval_sec"`
}

/**
 * 应用配置
 *
 * @author 王有政
 */
type AppConfig struct {
	AppName   string          `mapstructure:"app_name"`
	LogLevel  string          `mapstructure:"log_level"`
	OPCUA     OPCUAConfig     `mapstructure:"opcua"`
	NATS      NATSConfig      `mapstructure:"nats"`
	Collector CollectorConfig `mapstructure:"collector"`
}

func (c *AppConfig) Validate() error {
	if c.OPCUA.Endpoint == "" {
		return fmt.Errorf("OPC UA endpoint is required")
	}
	if c.NATS.URLs == "" {
		return fmt.Errorf("NATS URLs is required")
	}
	if c.Collector.WorkerCount <= 0 {
		c.Collector.WorkerCount = 10
	}
	if c.Collector.BatchSize <= 0 {
		c.Collector.BatchSize = 100
	}
	if c.Collector.ChannelBufferSize <= 0 {
		c.Collector.ChannelBufferSize = 10000
	}
	if c.Collector.StaleThresholdSec <= 0 {
		c.Collector.StaleThresholdSec = 10
	}
	if c.Collector.HeartbeatIntervalSec <= 0 {
		c.Collector.HeartbeatIntervalSec = 5
	}
	return nil
}
