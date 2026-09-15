/*
Package model 定义发送、回写和统计使用的数据结构。

采集状态由 collector.StateStore 持有。发送适配仍使用历史 DataPoint 类型承接
PointID、值、Quality 和时间；DataPoint.NodeID 在该链路中只是兼容字段名，内容为
协议无关 PointID。MQTT 与 NATS 都把 DataPoint 转换为相同的 BatchMessage JSON。

WriteCommand、BatchWriteItem 和 WriteResult 保持现有 OPC UA 回写契约。
*/
package model
