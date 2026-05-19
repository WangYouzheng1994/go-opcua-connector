// Package nats provides a NATS.io client for publishing and subscribing.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"sync"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"

	"go.uber.org/zap"
)

// Client NATS客户端，负责与NATS服务器的连接、发布和订阅
type Client struct {
	// config NATS配置
	config *config.NATSConfig
	// conn NATS底层连接
	conn *nats.Conn
	// logger 日志记录器
	logger *zap.Logger
	// mu 读写锁，保护连接和断开事件
	mu sync.RWMutex
	// connected 连接状态标识
	connected atomic.Bool
	// publishSeq 发布序号（预留）
	publishSeq atomic.Uint64
	// failCount 失败计数
	failCount atomic.Uint64
	// successCount 成功计数
	successCount atomic.Uint64
}

// NewClient 创建新的NATS客户端
func NewClient(cfg *config.NATSConfig, logger *zap.Logger) *Client {
	return &Client{
		config: cfg,
		logger: logger,
	}
}

// Connect 连接到NATS服务器
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

// Publish 发布单个数据点到指定topic
func (p *Client) Publish(ctx context.Context, topic string, point model.DataPoint) error {
	if !p.connected.Load() {
		return fmt.Errorf("not connected to NATS")
	}

	msg := model.NATSMessage{
		Topic:     topic,
		DataPoint: point,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	publishErr := p.conn.Publish(topic, data)

	if publishErr != nil {
		p.failCount.Add(1)
		return fmt.Errorf("failed to publish: %w", publishErr)
	}

	p.successCount.Add(1)
	return nil
}

// PublishBatch 批量发布数据点到指定topic
func (p *Client) PublishBatch(ctx context.Context, topic string, points []model.DataPoint) error {
	if !p.connected.Load() {
		return fmt.Errorf("not connected to NATS")
	}

	if len(points) == 0 {
		return nil
	}

	batchMsg := struct {
		Topic  string            `json:"topic"`
		Points []model.DataPoint `json:"points"`
	}{
		Topic:  topic,
		Points: points,
	}

	data, err := json.Marshal(batchMsg)
	if err != nil {
		p.failCount.Add(uint64(len(points)))
		return fmt.Errorf("failed to marshal batch message: %w", err)
	}

	if publishErr := p.conn.Publish(topic, data); publishErr != nil {
		p.failCount.Add(uint64(len(points)))
		return fmt.Errorf("failed to publish batch: %w", publishErr)
	}

	p.successCount.Add(uint64(len(points)))
	return nil
}

// GetStats 获取发布统计信息
func (p *Client) GetStats() (success, fail uint64) {
	return p.successCount.Load(), p.failCount.Load()
}

// Subscribe 订阅NATS主题，由回调函数处理消息
func (p *Client) Subscribe(ctx context.Context, subject string, handler nats.MsgHandler) (*nats.Subscription, error) {
	if !p.connected.Load() {
		return nil, fmt.Errorf("not connected to NATS")
	}

	// 订阅nats，以便接收从上到下的连接
	sub, err := p.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}

	p.logger.Info("Subscribed to NATS subject", zap.String("subject", subject))
	return sub, nil
}

// PublishRaw 发布原始字节数据到指定主题，不经过任何包装
func (p *Client) PublishRaw(subject string, data []byte) error {
	if !p.connected.Load() {
		return fmt.Errorf("not connected to NATS")
	}

	if err := p.conn.Publish(subject, data); err != nil {
		p.failCount.Add(1)
		return fmt.Errorf("failed to publish raw: %w", err)
	}

	p.successCount.Add(1)
	return nil
}

// IsConnected 检查是否已连接
func (p *Client) IsConnected() bool {
	return p.connected.Load()
}

// Close 关闭NATS连接
func (p *Client) Close() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
		p.connected.Store(false)
	}
	p.logger.Info("NATS client closed")
}
