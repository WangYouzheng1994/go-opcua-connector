// Package model defines core data structures for OPC UA data points and collector statistics.
package model

import "time"

// DataPoint OPC UA数据点模型
type DataPoint struct {
	// NodeID OPC UA节点ID
	NodeID string `json:"node_id"`
	// Value 数据点的实际值
	Value any `json:"value"`
	// Quality 品质，取值为 Good/Bad/Uncertain/Stale
	Quality string `json:"quality"`
	// Timestamp 数据时间戳
	Timestamp time.Time `json:"timestamp"`
	// Topic NATS主题
	Topic string `json:"topic"`
}

// NATSMessage NATS消息发布结构
type NATSMessage struct {
	// Topic NATS主题
	Topic string `json:"topic"`
	// DataPoint 数据点内容
	DataPoint DataPoint `json:"data_point"`
}

// NodeDataState 节点数据状态（用于心跳验证）
type NodeDataState struct {
	// NodeID 节点ID
	NodeID string
	// SubValue 订阅推送的最新值
	SubValue any
	// SubTimestamp 订阅推送的时间戳
	SubTimestamp time.Time
	// ReadValue 心跳拉取的最新值
	ReadValue any
	// ReadTimestamp 心跳拉取的时间戳
	ReadTimestamp time.Time
	// LastUpdate 最后更新时间，用于停滞判定
	LastUpdate time.Time
}

// CollectorStats 采集统计信息
type CollectorStats struct {
	// TotalPoints 累计采集总数
	TotalPoints int64 `json:"total_points"`
	// SuccessCount 成功发布数
	SuccessCount int64 `json:"success_count"`
	// FailureCount 失败数
	FailureCount int64 `json:"failure_count"`
	// StaleCount 停滞数据点数
	StaleCount int64 `json:"stale_count"`
	// LastSuccessTime 最后成功时间
	LastSuccessTime time.Time `json:"last_success_time"`
	// LastFailureTime 最后失败时间
	LastFailureTime time.Time `json:"last_failure_time"`
	// AvgLatencyMs 平均延迟，单位毫秒
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}