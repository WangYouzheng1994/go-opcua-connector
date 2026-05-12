/*
Package opcua 提供 OPC UA 客户端管理功能，负责连接 KepServer 并处理基于订阅的数据采集。

核心能力：
  1. Connect - 建立 TCP 连接 + OPC UA 会话
  2. Subscribe - 创建订阅和监控项，接收数据变化通知
  3. ResolveNodes - 浏览配置的文件夹节点，自动发现叶子变量
  4. Read/ReadAll - 主动拉取，用于心跳验证
  5. Close - 优雅断开连接

DataChangeHandler 回调类型将 OPC UA 通知桥接到采集器。

品质映射：
  status & 0xC0000000:
    0x00000000 -> "Good"
    0x40000000 -> "Uncertain"
    其他       -> "Bad"
*/
package opcua