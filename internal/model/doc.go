/*
Package model 定义应用核心数据结构。

类型说明：
  DataPoint:       OPC UA 数据点，包含节点ID、值、品质和时间戳
  NATSMessage:     JSON 发布消息，包装 DataPoint 并附带主题
  NodeDataState:   内部节点状态，用于心跳验证和停滞检测
                    - SubValue/SubTimestamp：来自订阅推送
                    - ReadValue/ReadTimestamp：来自心跳拉取
                    - LastUpdate：最新更新时间，用于停滞判定
  CollectorStats:  周期统计快照，用于监控

品质取值：
  "Good"       - 数据有效（StatusCode 高两位 0x00）
  "Bad"        - 数据无效（StatusCode 高两位 0x80）
  "Uncertain"  - 数据质量无法保证（StatusCode 高两位 0x40）
  "Stale"      - 超过停滞阈值未收到更新
  "BadNoValue" - 收到通知但 Value 字段为 nil
*/
package model