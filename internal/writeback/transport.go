// Package writeback implements the writeback engine for OPC UA write commands.
package writeback

import "context"

// Transport 回写传输层接口。
// 同一个中间件客户端同时实现 Publisher（给 Collector）和 Transport（给 Writeback）。
type Transport interface {
	Subscribe(ctx context.Context, subject string, handler func(data []byte)) (Subscription, error)
	PublishResult(ctx context.Context, subject string, data []byte) error
	Close()
}

// Subscription 订阅句柄。
type Subscription interface {
	Unsubscribe() error
}
