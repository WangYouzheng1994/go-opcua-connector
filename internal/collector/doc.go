/*
Package collector 实现数据采集引擎，采用订阅+拉取复合模式确保 OPC UA 数据的可靠采集。

架构分工：
  - ResolveNodes:      将配置的文件夹节点展开为叶子变量列表
  - Subscribe:         从 OPC UA 接收实时数据变化通知
  - subWorker:         将订阅数据分发到发布通道
  - heartbeatWorker:   定期拉取所有节点用于停滞检测
  - staleCheckWorker:  检测长时间未更新的节点，发出 "Stale" 品质
  - publishWorker:     批量缓存数据点并发布到 NATS

数据流：
  OPC UA 订阅 -> subCh -> subWorker -> pubCh -> publishWorker -> NATS
              \-> heartbeatWorker (ReadAll) -> 更新 LastUpdate

停滞检测：
  当节点最后更新时间超过 stale_threshold_sec 时，
  向 NATS 发送一条 Quality="Stale" 的数据点。
*/
package collector