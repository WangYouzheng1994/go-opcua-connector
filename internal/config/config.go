// Package config provides application configuration types, loading from YAML files with environment variable overrides.
package config

import "fmt"

/**
 * OPC UA服务器配置
 *
 * @author 王有政
 */
type OPCUAConfig struct {
	// OPC UA服务器端点地址，格式 opc.tcp://host:port
	Endpoint string `mapstructure:"endpoint"`
	// 客户端证书文件路径
	Certificate string `mapstructure:"certificate"`
	// 客户端私钥文件路径
	PrivateKey string `mapstructure:"private_key"`
	// 用户名
	Username string `mapstructure:"username"`
	// 密码
	Password string `mapstructure:"password"`
	// 安全策略，如 Basic256Sha256、Basic256、None
	SecurityPolicy string `mapstructure:"security_policy"`
	// 安全模式，可选值 None、Sign、SignAndEncrypt
	SecurityMode string `mapstructure:"security_mode"`
	// 连接超时时间，单位秒，默认30
	ConnectTimeout int `mapstructure:"connect_timeout"`
	// 请求超时时间，单位秒，默认30
	RequestTimeout int `mapstructure:"request_timeout"`
}

/**
 * NATS配置
 *
 * @author 王有政
 */
type NATSConfig struct {
	// NATS服务器地址，多个用逗号分隔
	URLs string `mapstructure:"urls"`
	// 用户名
	Username string `mapstructure:"username"`
	// 密码
	Password string `mapstructure:"password"`
	// 最大重连次数，-1表示无限重试
	MaxReconnects int `mapstructure:"max_reconnects"`
	// 重连等待时间，单位毫秒，默认1000
	ReconnectWaitMs int `mapstructure:"reconnect_wait_ms"`
}

/**
 * 采集器配置
 *
 * @author 王有政
 */
type CollectorConfig struct {
	// 并发worker数量，默认10
	WorkerCount int `mapstructure:"worker_count"`
	// 每批发布的数据点数量，默认100
	BatchSize int `mapstructure:"batch_size"`
	// 内部channel缓冲区大小，默认10000
	ChannelBufferSize int `mapstructure:"channel_buffer_size"`
	// 发布超时时间，单位毫秒，默认5000
	PublishTimeoutMs int `mapstructure:"publish_timeout_ms"`
	// 订阅的OPC UA节点ID列表，支持文件夹级自动展开
	SubscriptionNodes []string `mapstructure:"subscription_nodes"`
	// NATS订阅主题，默认 opcua/data
	SubscriptionTopic string `mapstructure:"subscription_topic"`
	// 监控统计输出间隔，单位秒，默认60
	MonitorIntervalSec int `mapstructure:"monitor_interval_sec"`
	// 数据停滞判定阈值，单位秒，默认10
	StaleThresholdSec int `mapstructure:"stale_threshold_sec"`
	// 心跳验证拉取间隔，单位秒，默认5
	HeartbeatIntervalSec int `mapstructure:"heartbeat_interval_sec"`
}

/**
 * 应用配置
 *
 * @author 王有政
 */
type AppConfig struct {
	// 应用名称，默认 go-opcua-connector
	AppName string `mapstructure:"app_name"`
	// 日志级别，可选 debug/info/warn/error，默认 info
	LogLevel string `mapstructure:"log_level"`
	// OPC UA服务器配置
	OPCUA OPCUAConfig `mapstructure:"opcua"`
	// NATS服务器配置
	NATS NATSConfig `mapstructure:"nats"`
	// 采集器配置
	Collector CollectorConfig `mapstructure:"collector"`
}

/**
 * 验证应用配置的必填项，为缺失的数值型配置设置默认值
 */
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
