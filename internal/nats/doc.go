/*
Package nats 提供 NATS.io 消息发布功能，用于转发 OPC UA 数据点。

支持能力：
  - 带认证和自动重连的连接管理，含事件处理器
  - 单条消息发布（Publish）
  - 批量消息发布（PublishBatch），单条失败不影响其余
  - 通过 atomic.Bool 跟踪连接状态
  - 发布成功/失败计数器

Publisher 类型封装 nats.Conn，为连接/断开/重连事件提供结构化日志。

配置项：
  urls:               NATS 服务器地址（多个用逗号分隔）
  max_reconnects:     最大重连次数（-1 表示无限）
  reconnect_wait_ms:  重连等待间隔，单位毫秒
*/
package nats