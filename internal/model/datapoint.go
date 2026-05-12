// Package model defines core data structures for OPC UA data points and collector statistics.
package model

import "time"

/**
 * OPC UA数据点模型
 *
 * @author 王有政
 */
type DataPoint struct {
	// OPC UA节点ID
	NodeID string `json:"node_id"`
	// 数据点的实际值
	Value any `json:"value"`
	// 品质，取值为 Good/Bad/Uncertain/Stale
	Quality string `json:"quality"`
	// 数据时间戳
	Timestamp time.Time `json:"timestamp"`
	// NATS主题
	Topic string `json:"topic"`
}

/**
 * NATS消息发布结构
 *
 * @author 王有政
 */
type NATSMessage struct {
	// NATS主题
	Topic string `json:"topic"`
	// 数据点内容
	DataPoint DataPoint `json:"data_point"`
}

/**
 * 节点数据状态（用于心跳验证）
 *
 * @author 王有政
 */
type NodeDataState struct {
	// 节点ID
	NodeID string
	// 订阅推送的最新值
	SubValue any
	// 订阅推送的时间戳
	SubTimestamp time.Time
	// 心跳拉取的最新值
	ReadValue any
	// 心跳拉取的时间戳
	ReadTimestamp time.Time
	// 最后更新时间，用于停滞判定
	LastUpdate time.Time
}

/**
 * 采集统计信息
 *
 * @author 王有政
 */
type CollectorStats struct {
	// 累计采集总数
	TotalPoints int64 `json:"total_points"`
	// 成功发布数
	SuccessCount int64 `json:"success_count"`
	// 失败数
	FailureCount int64 `json:"failure_count"`
	// 停滞数据点数
	StaleCount int64 `json:"stale_count"`
	// 最后成功时间
	LastSuccessTime time.Time `json:"last_success_time"`
	// 最后失败时间
	LastFailureTime time.Time `json:"last_failure_time"`
	// 平均延迟，单位毫秒
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}
