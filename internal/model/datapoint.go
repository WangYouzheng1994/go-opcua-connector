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

// NodeDataState 节点数据状态（内存池中的统一状态）
type NodeDataState struct {
	// NodeID 节点ID
	NodeID string
	// Value 当前值
	Value any
	// Quality 品质，取值为 Good/Bad/Uncertain/Stale
	Quality string
	// Timestamp 最新值的时间戳
	Timestamp time.Time
	// Status 节点状态：Online（正常）/ Stale（停滞）/ Error（错误）
	Status string
	// DataType OPC UA数据类型：String/Int32/Int64/Float32/Float64/Bool/Double 等
	// 用于回写时自动类型适配
	DataType string
	// Dirty 是否需要推送（仅定时模式使用）
	Dirty bool
}

// WriteCommand NATS回写命令，由外部系统通过NATS发送
type WriteCommand struct {
	// NodeID 目标OPC UA节点ID
	NodeID string `json:"node_id"`
	// Value 待写入的原始值，通常为字符串形式，回写时会进行类型转换
	Value string `json:"value"`
	// ValueType 期望的目标类型，可选 int32/int64/float32/float64/bool/string
	// 为空时自动推断：先尝试数值，再尝试布尔，最后作为字符串
	ValueType string `json:"value_type,omitempty"`
	// RequestID 请求标识，用于结果关联
	RequestID string `json:"request_id,omitempty"`
}

// WriteResult NATS回写结果
type WriteResult struct {
	// NodeID 目标节点ID
	NodeID string `json:"node_id"`
	// RequestID 对应的请求标识
	RequestID string `json:"request_id,omitempty"`
	// Success 是否成功
	Success bool `json:"success"`
	// WrittenValue 实际写入的值
	WrittenValue any `json:"written_value,omitempty"`
	// Error 错误信息
	Error string `json:"error,omitempty"`
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
	// DroppedPoints 丢弃的数据点数
	DroppedPoints int64 `json:"dropped_points"`
	// LastSuccessTime 最后成功时间
	LastSuccessTime time.Time `json:"last_success_time"`
	// LastFailureTime 最后失败时间
	LastFailureTime time.Time `json:"last_failure_time"`
	// AvgLatencyMs 平均延迟，单位毫秒
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}