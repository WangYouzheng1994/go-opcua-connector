package model

import "time"

/**
 * OPC UA数据点模型
 *
 * @author 王有政
 */
type DataPoint struct {
	NodeID    string    `json:"node_id"`
	Value     any       `json:"value"`
	Quality   string    `json:"quality"`
	Timestamp time.Time `json:"timestamp"`
	Topic     string    `json:"topic"`
}

/**
 * NATS消息发布结构
 *
 * @author 王有政
 */
type NATSMessage struct {
	Topic     string    `json:"topic"`
	DataPoint DataPoint `json:"data_point"`
}

/**
 * 节点数据状态（用于心跳验证）
 *
 * @author 王有政
 */
type NodeDataState struct {
	NodeID       string
	SubValue     any
	SubTimestamp time.Time
	ReadValue    any
	ReadTimestamp time.Time
	LastUpdate   time.Time
}

/**
 * 采集统计信息
 *
 * @author 王有政
 */
type CollectorStats struct {
	TotalPoints      int64     `json:"total_points"`
	SuccessCount     int64     `json:"success_count"`
	FailureCount     int64     `json:"failure_count"`
	StaleCount       int64     `json:"stale_count"`
	LastSuccessTime  time.Time `json:"last_success_time"`
	LastFailureTime  time.Time `json:"last_failure_time"`
	AvgLatencyMs     float64   `json:"avg_latency_ms"`
}
