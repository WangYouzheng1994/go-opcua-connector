// Package nats provides NATS.io publishing capabilities for data points.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

/**
 * NATS发布者
 * 负责将数据点发布到NATS.io
 *
 * @author 王有政
 */
type Publisher struct {
	// NATS配置
	config *config.NATSConfig
	// NATS底层连接
	conn *nats.Conn
	// 日志记录器
	logger *zap.Logger
	// 读写锁，保护连接和断开事件
	mu sync.RWMutex
	// 连接状态标识
	connected atomic.Bool
	// 发布序号（预留）
	publishSeq atomic.Uint64
	// 失败计数
	failCount atomic.Uint64
	// 成功计数
	successCount atomic.Uint64
}

/**
 * 创建新的NATS发布者
 *
 */
func NewPublisher(cfg *config.NATSConfig, logger *zap.Logger) *Publisher {
	return &Publisher{
		config: cfg,
		logger: logger,
	}
}

/**
 * 连接到NATS服务器
 *
 */
func (p *Publisher) Connect(ctx context.Context) error {
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

/**
 * 发布单个数据点到指定topic
 *
 */
func (p *Publisher) Publish(ctx context.Context, topic string, point model.DataPoint) error {
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

/**
 * 批量发布数据点到指定topic
 *
 */
func (p *Publisher) PublishBatch(ctx context.Context, topic string, points []model.DataPoint) error {
	if !p.connected.Load() {
		return fmt.Errorf("not connected to NATS")
	}

	if len(points) == 0 {
		return nil
	}

	for i := range points {
		msg := model.NATSMessage{
			Topic:     topic,
			DataPoint: points[i],
		}

		data, err := json.Marshal(msg)
		if err != nil {
			p.failCount.Add(1)
			continue
		}

		publishErr := p.conn.Publish(topic, data)

		if publishErr != nil {
			p.failCount.Add(1)
		} else {
			p.successCount.Add(1)
		}
	}

	return nil
}

/**
 * 获取发布统计信息
 *
 */
func (p *Publisher) GetStats() (success, fail uint64) {
	return p.successCount.Load(), p.failCount.Load()
}

/**
 * 检查是否已连接
 *
 */
func (p *Publisher) IsConnected() bool {
	return p.connected.Load()
}

/**
 * 关闭NATS连接
 *
 */
func (p *Publisher) Close() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
		p.connected.Store(false)
	}
	p.logger.Info("NATS publisher closed")
}
