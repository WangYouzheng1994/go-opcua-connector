/*
Package collector 实现协议无关的数据采集、状态管理和发送适配。

生产数据流：

	SourceAdapter.Discover
	  -> StateStore.Initialize
	  -> Subscription / Initial Verification
	  -> StateStore
	  -> immediate 变化快照或 timed 全量快照
	  -> Publisher

订阅是运行期 Value 的唯一更新来源。主动读取只负责初值播种、可信度验证和
静态值确认。ConnectionGeneration 隔离连接生命周期，MonitorGeneration 隔离
单点监控生命周期。

点位发现失败按 Source 和 Reason 输出诊断；空点表导致启动或恢复失败时，返回错误也保留发现原因。
统计日志区分当前状态池点位数、完整健康状态分布、累计成功发布数和累计发布失败数。
连接监督在源连接首次不可用、开始完整恢复和恢复成功时记录连接代次与点位数量；恢复失败记录原因和重试间隔。

Collector 只把 PointSnapshot 转换为现有 model.DataPoint 发送契约，不保存 OPC UA
NodeID、BrowsePath 或 StatusCode。即时模式消费 StateStore 的合并变化信号，定时
模式按周期读取当前全量快照；两种模式都在质量过滤后按 BatchSize 分批发布。
*/
package collector
