package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

/**
 * 配置加载器
 *
 * @author 王有政
 */
type Loader struct {
	// 配置文件搜索路径
	configPath string
	// 配置文件名称（不含扩展名）
	configName string
}

/**
 * 创建配置加载器实例
 */
func NewLoader(configPath, configName string) *Loader {
	return &Loader{
		configPath: configPath,
		configName: configName,
	}
}

/**
 * 加载并解析配置文件，绑定环境变量覆盖，返回应用配置
 */
func (l *Loader) Load() (*AppConfig, error) {
	v := viper.New()

	if l.configPath != "" {
		v.AddConfigPath(l.configPath)
	}

	v.SetConfigName(l.configName)

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("app_name", "go-opcua-connector")
	v.SetDefault("log_level", "info")
	v.SetDefault("opcua.connect_timeout", 30)
	v.SetDefault("opcua.request_timeout", 30)
	v.SetDefault("nats.max_reconnects", -1)
	v.SetDefault("nats.reconnect_wait_ms", 1000)
	v.SetDefault("collector.worker_count", 10)
	v.SetDefault("collector.batch_size", 100)
	v.SetDefault("collector.channel_buffer_size", 10000)
	v.SetDefault("collector.publish_timeout_ms", 5000)
	v.SetDefault("collector.monitor_interval_sec", 60)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
