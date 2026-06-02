// Package nats provides a NATS.io client implementing collector.Publisher and writeback.Transport.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
	"go-opcua-connector/internal/writeback"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var _ writeback.Transport = (*Client)(nil)

// Client NATS客户端，同时实现 collector.Publisher 和 writeback.Transport。
type Client struct {
	config    *config.NATSConfig
	conn      *nats.Conn
	logger    *zap.Logger
	connected atomic.Bool
}

// NewClient 创建新的NATS客户端。
func NewClient(cfg *config.NATSConfig, logger *zap.Logger) *Client {
	return &Client{
		config: cfg,
		logger: logger,
	}
}

// Connect 连接到NATS服务器。
func (p *Client) Connect(ctx context.Context) error {
	opts := []nats.Option{
		nats.Name("go-opcua-connector"),
		nats.MaxReconnects(p.config.MaxReconnects),
		nats.ReconnectWait(time.Duration(p.config.ReconnectWaitMs) * time.Millisecond),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			p.logger.Warn("NATS disconnected", zap.Error(err))
			p.connected.Store(false)
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			p.logger.Info("NATS reconnected")
			p.connected.Store(true)
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			p.logger.Warn("NATS connection closed")
			p.connected.Store(false)
		}),
	}

	if p.config.Username != "" && p.config.Password != "" {
		opts = append(opts, nats.UserInfo(p.config.Username, p.config.Password))
	}

	conn, err := nats.Connect(p.config.URLs, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	p.conn = conn
	p.connected.Store(true)

	p.logger.Info("Connected to NATS", zap.String("urls", p.config.URLs))

	return nil
}

// PublishBatch 批量发布数据点，实现 collector.Publisher。
// 通过 goroutine+select 响应 ctx 的超时和取消信号。
func (p *Client) PublishBatch(ctx context.Context, topic string, points []model.DataPoint) error {
	if !p.connected.Load() {
		return fmt.Errorf("not connected to NATS")
	}

	if len(points) == 0 {
		return nil
	}

	batchMsg := model.NewBatchMessage(points)

	data, err := json.Marshal(batchMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal batch message: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- p.conn.Publish(topic, data)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsConnected 检查连接状态。
func (p *Client) IsConnected() bool {
	return p.connected.Load()
}

// Subscribe 订阅NATS主题，实现 writeback.Transport。
func (p *Client) Subscribe(ctx context.Context, subject string, handler func(data []byte)) (writeback.Subscription, error) {
	if !p.connected.Load() {
		return nil, fmt.Errorf("not connected to NATS")
	}

	sub, err := p.conn.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}

	p.logger.Info("Subscribed to NATS subject", zap.String("subject", subject))
	return &natsSubscription{sub: sub}, nil
}

// PublishResult 发布回写结果，实现 writeback.Transport。
func (p *Client) PublishResult(ctx context.Context, subject string, data []byte) error {
	if !p.connected.Load() {
		return fmt.Errorf("not connected to NATS")
	}

	done := make(chan error, 1)
	go func() {
		done <- p.conn.Publish(subject, data)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close 关闭NATS连接。
func (p *Client) Close() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
		p.connected.Store(false)
	}
	p.logger.Info("NATS client closed")
}

// natsSubscription 包装 *nats.Subscription 为 writeback.Subscription。
type natsSubscription struct {
	sub *nats.Subscription
}

func (s *natsSubscription) Unsubscribe() error {
	return s.sub.Unsubscribe()
}
