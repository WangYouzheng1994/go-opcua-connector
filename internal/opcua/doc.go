/*
Package opcua 提供 OPC UA 客户端管理功能，负责连接 KepServer 并处理基于订阅的数据采集。

生产采集链路的核心能力：
 1. Connect - 建立 TCP 连接和 OPC UA 会话
 2. DiscoverPoints - 建立 PointID、SourceRef 和完整 BrowsePath 私有映射
 3. SubscribePoints - 逐项检查监控结果并输出标准化 PointSample
 4. ReadInitial - 将读取结果转换为仅供播种和验证的 Verification
 5. AcquisitionAdapter - 向 collector 提供协议无关采集边界和真实连接状态
 6. Close - 由应用生命周期关闭共享客户端

生产入口通过 AcquisitionAdapter 使用上述链路。PointID 进入 StateStore 和发送层，
完整 NodeID 只作为本包私有 SourceRef 参与 Browse、订阅、读取和回写。
解析完整 BrowsePath 时先查询反向层级引用；找不到路径时从 Objects 正向浏览，
兼容业务节点不返回反向引用的服务端，仍以实际 BrowseName 生成 PointID。

品质映射：

	status & 0xC0000000:
	  0x00000000 -> "Good"
	  0x40000000 -> "Uncertain"
	  其他       -> "Bad"
*/
package opcua
