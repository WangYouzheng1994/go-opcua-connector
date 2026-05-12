/*
Package config 提供应用配置的加载、解析和验证功能。

通过 viper 从 config.yaml 加载配置，支持环境变量覆盖。
所有配置结构体使用 mapstructure 标签与 viper 绑定。

配置层次：
  app_name:      应用标识
  log_level:     日志级别（debug/info/warn/error）
  opcua:         OPC UA 服务器连接参数
  nats:          NATS.io 消息中间件连接参数
  collector:     数据采集器运行参数

Loader 类型处理 YAML 文件读取和反序列化。
AppConfig.Validate() 为缺省的数值类型字段设置默认值。
*/
package config