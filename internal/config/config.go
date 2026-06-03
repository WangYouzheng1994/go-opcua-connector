// Package config provides application configuration types, loading from YAML files with environment variable overrides.
package config

import "fmt"

// OutputType 输出目标类型
type OutputType string

const (
	// OutputTypeNATS 推送到NATS
	OutputTypeNATS OutputType = "nats"
	// OutputTypeMQTT 推送到MQTT
	OutputTypeMQTT OutputType = "mqtt"
)

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

// PushModeType 推送模式类型
type PushModeType string

const (
	// PushModeImmediate 即时推送：每次变化立即推送，同时支持心跳强制推送
	PushModeImmediate PushModeType = "immediate"
	// PushModeTimed 定时推送：累积变化，定时批量推送全量快照
	PushModeTimed PushModeType = "timed"
)

// MQTTConfig MQTT配置，仅 output_type=mqtt 时生效
type MQTTConfig struct {
	Brokers              string `mapstructure:"brokers"`
	ClientID             string `mapstructure:"client_id"`
	Username             string `mapstructure:"username"`
	Password             string `mapstructure:"password"`
	Topic                string `mapstructure:"topic"`
	Qos                  int    `mapstructure:"qos"`
	Retained             bool   `mapstructure:"retained"`
	CleanSession         bool   `mapstructure:"clean_session"`
	KeepAliveSec         int    `mapstructure:"keepalive_sec"`
	ConnectTimeoutSec    int    `mapstructure:"connect_timeout_sec"`
	PublishTimeoutSec    int    `mapstructure:"publish_timeout_sec"`
	AutoReconnect        bool   `mapstructure:"auto_reconnect"`
	MaxReconnectDelaySec int    `mapstructure:"max_reconnect_delay_sec"`
	TLSCertFile          string `mapstructure:"tls_cert_file"`
	TLSKeyFile           string `mapstructure:"tls_key_file"`
	TLSCAFile            string `mapstructure:"tls_ca_file"`
	InsecureSkipVerify   bool   `mapstructure:"insecure_skip_verify"`
}

// CollectorConfig 采集器配置
type CollectorConfig struct {
	// OutputType 输出目标类型：nats / mqtt，默认 nats
	OutputType OutputType `mapstructure:"output_type"`
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
	// SubscriptionTopic 发布主题，默认 opcua/data
	SubscriptionTopic string `mapstructure:"subscription_topic"`
	// MonitorIntervalSec 监控统计输出间隔，单位秒，默认60
	MonitorIntervalSec int `mapstructure:"monitor_interval_sec"`
	// StaleThresholdSec 数据停滞判定阈值，单位秒，默认30
	StaleThresholdSec int `mapstructure:"stale_threshold_sec"`
	// HeartbeatIntervalSec 心跳验证拉取间隔，单位秒，默认5
	HeartbeatIntervalSec int `mapstructure:"heartbeat_interval_sec"`
	// PushMode 推送模式：immediate（即时推送）或 timed（定时批量推送），默认 timed
	PushMode PushModeType `mapstructure:"push_mode"`
	// PushIntervalSec 定时模式下的推送间隔，单位秒，默认1
	PushIntervalSec int `mapstructure:"push_interval_sec"`
	// ForceHeartbeat 即时模式下是否强制心跳推送，默认true
	ForceHeartbeat bool `mapstructure:"force_heartbeat"`
}

// WritebackConfig 回写配置
type WritebackConfig struct {
	// WriteSubject 接收回写命令的主题，默认 opcua/write
	WriteSubject string `mapstructure:"write_subject"`
	// ResultSubject 发布回写结果的主题，默认 opcua/write/result
	ResultSubject string `mapstructure:"result_subject"`
}

// LogConfig 日志配置
type LogConfig struct {
	// Level 日志级别，可选 debug/info/warn/error，默认 info
	Level string `mapstructure:"level"`
	// Dir 日志文件存放目录，默认 ./logs
	Dir string `mapstructure:"dir"`
	// MaxAgeDay 日志保留天数，默认 3
	MaxAgeDay int `mapstructure:"max_age_day"`
	// MaxSizeMB 单个日志文件最大大小（MB），默认 100
	MaxSizeMB int `mapstructure:"max_size_mb"`
}

// AppConfig 应用配置
type AppConfig struct {
	// AppName 应用名称，默认 go-opcua-connector
	AppName string `mapstructure:"app_name"`
	// Log 日志配置
	Log LogConfig `mapstructure:"log"`
	// OPCUA OPC UA服务器配置
	OPCUA OPCUAConfig `mapstructure:"opcua"`
	// NATS NATS服务器配置
	NATS NATSConfig `mapstructure:"nats"`
	MQTT MQTTConfig `mapstructure:"mqtt"`
	// Collector 采集器配置
	Collector CollectorConfig `mapstructure:"collector"`
	// Writeback 回写配置
	Writeback WritebackConfig `mapstructure:"writeback"`
}

// Validate 验证应用配置的必填项，为缺失的数值型配置设置默认值
func (c *AppConfig) Validate() error {
	if c.OPCUA.Endpoint == "" {
		return fmt.Errorf("OPC UA endpoint is required")
	}

	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Dir == "" {
		c.Log.Dir = "./logs"
	}
	if c.Log.MaxAgeDay <= 0 {
		c.Log.MaxAgeDay = 3
	}
	if c.Log.MaxSizeMB <= 0 {
		c.Log.MaxSizeMB = 100
	}

	if c.Collector.OutputType == "" {
		c.Collector.OutputType = OutputTypeNATS
	}

	switch c.Collector.OutputType {
	case OutputTypeNATS:
		if c.NATS.URLs == "" {
			return fmt.Errorf("NATS URLs is required when output_type is nats")
		}
	case OutputTypeMQTT:
		if c.MQTT.Brokers == "" {
			return fmt.Errorf("MQTT brokers is required when output_type is mqtt")
		}
	default:
		return fmt.Errorf("invalid output_type: %s, must be nats / mqtt", c.Collector.OutputType)
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
	if c.Collector.PushMode == "" {
		c.Collector.PushMode = PushModeTimed
	}
	if c.Collector.PushIntervalSec <= 0 {
		c.Collector.PushIntervalSec = 1
	}
	if c.Writeback.WriteSubject == "" {
		c.Writeback.WriteSubject = "opcua/write"
	}
	if c.Writeback.ResultSubject == "" {
		c.Writeback.ResultSubject = "opcua/write/result"
	}
	if c.MQTT.Topic == "" {
		c.MQTT.Topic = "opcua/data"
	}
	if c.MQTT.KeepAliveSec <= 0 {
		c.MQTT.KeepAliveSec = 60
	}
	if c.MQTT.ConnectTimeoutSec <= 0 {
		c.MQTT.ConnectTimeoutSec = 10
	}
	if c.MQTT.PublishTimeoutSec <= 0 {
		c.MQTT.PublishTimeoutSec = 5
	}
	if c.MQTT.MaxReconnectDelaySec <= 0 {
		c.MQTT.MaxReconnectDelaySec = 60
	}
	return nil
}
