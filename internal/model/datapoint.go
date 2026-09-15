// Package model defines core data structures for OPC UA data points and collector statistics.
package model

import (
	"fmt"
	"strings"
	"time"
)

// DataPoint OPC UA数据点模型
type DataPoint struct {
	// NodeID 是历史字段名；采集发送链路中保存协议无关 PointID，不保存 OPC UA NodeID。
	NodeID    string    `json:"node_id"`
	Value     any       `json:"value"`
	Quality   string    `json:"quality"`
	Timestamp time.Time `json:"timestamp"`
}

// BatchPoint 批量推送中的单条数据点（精简字段名）
type BatchPoint struct {
	ID string `json:"id"`
	V  any    `json:"v"`
	Q  bool   `json:"q"`
	T  int64  `json:"t"`
}

// BatchMessage 批量推送消息结构
type BatchMessage struct {
	Timestamp int64        `json:"timestamp"`
	Values    []BatchPoint `json:"values"`
}

// NewBatchMessage 将DataPoint列表转换为批量推送消息。
func NewBatchMessage(points []DataPoint) BatchMessage {
	nowMs := time.Now().UnixMilli()
	values := make([]BatchPoint, len(points))
	for i, p := range points {
		values[i] = BatchPoint{
			ID: p.NodeID,
			V:  p.Value,
			Q:  p.Quality == "Good",
			T:  nowMs,
		}
	}
	return BatchMessage{
		Timestamp: nowMs,
		Values:    values,
	}
}

// WriteCommand 回写命令，由外部系统通过消息队列发送
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

// BatchWriteItem 批量回写命令中的单条（精简字段名，与推送格式对应）
type BatchWriteItem struct {
	ID string `json:"id"`
	V  any    `json:"v"`
}

// ToWriteCommand 转换为内部 WriteCommand，nodeIDPrefix 用于还原完整节点ID。
func (b BatchWriteItem) ToWriteCommand(nodeIDPrefix string) WriteCommand {
	nodeID := b.ID
	if nodeIDPrefix != "" && !strings.Contains(nodeID, ";s=") {
		nodeID = nodeIDPrefix + nodeID
	}
	value := ""
	switch v := b.V.(type) {
	case string:
		value = v
	case float64:
		value = fmt.Sprintf("%v", v)
	default:
		value = fmt.Sprintf("%v", v)
	}
	return WriteCommand{
		NodeID: nodeID,
		Value:  value,
	}
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
	// CurrentPoints 当前状态池中的点位数
	CurrentPoints int64 `json:"current_points"`
	// HealthyPoints 当前健康点位数
	HealthyPoints int64 `json:"healthy_points"`
	// UnhealthyPoints 当前非健康点位数
	UnhealthyPoints int64 `json:"unhealthy_points"`
	// InitializingPoints 当前初始化中的点位数
	InitializingPoints int64 `json:"initializing_points"`
	// SampleInvalidPoints 当前订阅样本无效的点位数
	SampleInvalidPoints int64 `json:"sample_invalid_points"`
	// SubscribeFailedPoints 当前订阅失败的点位数
	SubscribeFailedPoints int64 `json:"subscribe_failed_points"`
	// VerificationFailedPoints 当前主动验证失败的点位数
	VerificationFailedPoints int64 `json:"verification_failed_points"`
	// SubscriptionMismatchPoints 当前订阅值与主动读取值不一致的点位数
	SubscriptionMismatchPoints int64 `json:"subscription_mismatch_points"`
	// SourceDisconnectedPoints 当前因数据源断开而失效的点位数
	SourceDisconnectedPoints int64 `json:"source_disconnected_points"`
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
