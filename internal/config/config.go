// Package config provides application configuration types, loading from YAML files with environment variable overrides.
package config

import "fmt"

// OPCUAConfig OPC UA服务器配置
type OPCUAConfig struct {
	// Endpoint OPC UA服务器端点地址，格式 opc.tcp://host:port
	Endpoint string `mapstructure:"endpoint"`
	// Certificate 客户端证书文件路径
	Certificate string `mapstructure:"certificate"`
	// PrivateKey 客户端私钥文件路径
	PrivateKey string `mapstructure:"private_key"`
	// Username 用户名
	Username string `mapstructure:"username"`
	// Password 密码
	Password string `mapstructure:"password"`
	// SecurityPolicy 安全策略，如 Basic256Sha256、Basic256、None
	SecurityPolicy string `mapstructure:"security_policy"`
	// SecurityMode 安全模式，可选值 None、Sign、SignAndEncrypt
	SecurityMode string `mapstructure:"security_mode"`
	// ConnectTimeout 连接超时时间，单位秒，默认30
	ConnectTimeout int `mapstructure:"connect_timeout"`
	// RequestTimeout 请求超时时间，单位秒，默认30
	RequestTimeout int `mapstructure:"request_timeout"`
}

// NATSConfig NATS配置
type NATSConfig struct {
	// URLs NATS服务器地址，多个用逗号分隔
	URLs string `mapstructure:"urls"`
	// Username 用户名
	Username string `mapstructure:"username"`
	// Password 密码
	Password string `mapstructure:"password"`
	// MaxReconnects 最大重连次数，-1表示无限重试
	MaxReconnects int `mapstructure:"max_reconnects"`
	// ReconnectWaitMs 重连等待时间，单位毫秒，默认1000
	ReconnectWaitMs int `mapstructure:"reconnect_wait_ms"`
}

// CollectorConfig 采集器配置
type CollectorConfig struct {
	// WorkerCount 并发worker数量，默认10
	WorkerCount int `mapstructure:"worker_count"`
	// BatchSize 每批发布的数据点数量，默认100
	BatchSize int `mapstructure:"batch_size"`
	// ChannelBufferSize 内部channel缓冲区大小，默认10000
	ChannelBufferSize int `mapstructure:"channel_buffer_size"`
	// PublishTimeoutMs 发布超时时间，单位毫秒，默认5000
	PublishTimeoutMs int `mapstructure:"publish_timeout_ms"`
	// SubscriptionNodes 订阅的OPC UA节点ID列表，支持文件夹级自动展开
	SubscriptionNodes []string `mapstructure:"subscription_nodes"`
	// SubscriptionTopic NATS订阅主题，默认 opcua/data
	SubscriptionTopic string `mapstructure:"subscription_topic"`
	// MonitorIntervalSec 监控统计输出间隔，单位秒，默认60
	MonitorIntervalSec int `mapstructure:"monitor_interval_sec"`
	// StaleThresholdSec 数据停滞判定阈值，单位秒，默认10
	StaleThresholdSec int `mapstructure:"stale_threshold_sec"`
	// HeartbeatIntervalSec 心跳验证拉取间隔，单位秒，默认5
	HeartbeatIntervalSec int `mapstructure:"heartbeat_interval_sec"`
}

// AppConfig 应用配置
type AppConfig struct {
	// AppName 应用名称，默认 go-opcua-connector
	AppName string `mapstructure:"app_name"`
	// LogLevel 日志级别，可选 debug/info/warn/error，默认 info
	LogLevel string `mapstructure:"log_level"`
	// OPCUA OPC UA服务器配置
	OPCUA OPCUAConfig `mapstructure:"opcua"`
	// NATS NATS服务器配置
	NATS NATSConfig `mapstructure:"nats"`
	// Collector 采集器配置
	Collector CollectorConfig `mapstructure:"collector"`
}

// Validate 验证应用配置的必填项，为缺失的数值型配置设置默认值
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