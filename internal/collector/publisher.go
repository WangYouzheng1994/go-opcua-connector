// Package collector implements the data collection engine with memory pool and dual push modes.
package collector

import (
	"context"

	"go-opcua-connector/internal/model"
)

// Publisher 发布器接口。
// Collector 通过此接口发布采集数据，不关心底层是 NATS、MQTT 还是其他中间件。
// 实现方必须响应 ctx 的超时和取消信号。
type Publisher interface {
	PublishBatch(ctx context.Context, topic string, points []model.DataPoint) error
	IsConnected() bool
	Close()
}
