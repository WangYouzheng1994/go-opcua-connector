/*
Package nats 提供 NATS.io 消息客户端，用于发布 OPC UA 数据点和订阅回写命令。

支持能力：
  - 带认证和自动重连的连接管理，含事件处理器
  - 单条消息发布（Publish）
  - 批量消息发布（PublishBatch），整个批次一次性序列化和发送
  - 主题订阅（Subscribe）
  - 原始字节发布（PublishRaw）
  - 通过 atomic.Bool 跟踪连接状态
  - 发布成功/失败计数器

Client 类型封装 nats.Conn，一个连接同时承载发布和订阅操作。

配置项：
  urls:               NATS 服务器地址（多个用逗号分隔）
  max_reconnects:     最大重连次数（-1 表示无限）
  reconnect_wait_ms:  重连等待间隔，单位毫秒
*/
package nats