// Package collector implements the data collection engine with subscription + polling composite mode.
package collector

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
	"go-opcua-connector/internal/nats"
	"go-opcua-connector/internal/opcua"

	"go.uber.org/zap"
)

// Collector 数据采集器
// 订阅+拉取复合模式：
// - 订阅：实时接收数据变化
// - 拉取：定期心跳验证
// - 合并：统一判定Quality后发布到NATS
type Collector struct {
	// config 采集器配置
	config *config.CollectorConfig
	// opcuaClient OPC UA客户端
	opcuaClient *opcua.Client
	// natsClient NATS客户端
	natsClient *nats.Client
	// logger 日志记录器
	logger *zap.Logger

	// subCh 订阅数据输入通道
	subCh chan []model.DataPoint
	// pubCh 发布数据输出通道
	pubCh chan model.DataPoint
	// wg 等待组，管理所有worker协程
	wg sync.WaitGroup
	// ctx 根上下文
	ctx context.Context
	// cancel 取消函数
	cancel context.CancelFunc

	// nodeStates 节点数据状态表，key为NodeID, value为NodeDataState对象的指针
	nodeStates map[string]*model.NodeDataState
	// nodeStatesMu 保护nodeStates的读写锁
	nodeStatesMu sync.RWMutex

	// stats 统计信息快照
	stats atomic.Value
	// startTime 启动时间
	startTime time.Time
	// totalPoints 累计点数
	totalPoints atomic.Int64
	// successCount 成功计数
	successCount atomic.Int64
	// failCount 失败计数
	failCount atomic.Int64
	// staleCount 停滞计数
	staleCount atomic.Int64
}

// New 创建采集器实例
func New(
	cfg *config.CollectorConfig,
	opcuaClient *opcua.Client,
	natsClient *nats.Client,
	logger *zap.Logger,
) *Collector {
	ctx, cancel := context.WithCancel(context.Background())

	return &Collector{
		config:      cfg,
		opcuaClient: opcuaClient,
		natsClient:  natsClient,
		logger:      logger,
		subCh:       make(chan []model.DataPoint, cfg.ChannelBufferSize),
		pubCh:       make(chan model.DataPoint, cfg.ChannelBufferSize),
		ctx:         ctx,
		cancel:      cancel,
		nodeStates:  make(map[string]*model.NodeDataState),
		startTime:   time.Now(),
	}
}

// Start 启动采集器
func (c *Collector) Start() error {
	c.logger.Info("Starting collector",
		zap.Int("worker_count", c.config.WorkerCount),
		zap.Int("batch_size", c.config.BatchSize),
		zap.Int("channel_buffer", c.config.ChannelBufferSize),
		zap.Int("stale_threshold_sec", c.config.StaleThresholdSec),
		zap.Int("heartbeat_interval_sec", c.config.HeartbeatIntervalSec))

	resolvedNodes, err := c.opcuaClient.ResolveNodes(c.ctx, c.config.SubscriptionNodes)
	if err != nil {
		return fmt.Errorf("failed to resolve nodes: %w", err)
	}

	c.logger.Info("Nodes resolved",
		zap.Int("configured_count", len(c.config.SubscriptionNodes)),
		zap.Int("resolved_count", len(resolvedNodes)))

	c.nodeStatesMu.Lock()
	// 遍历读取到的点位信息，按照点位id 和 点位对象(nodeId, 上次更新时间前推1小时，为了让项目启动后立刻触发心跳)进行保存
	for _, nodeID := range resolvedNodes {
		c.nodeStates[nodeID] = &model.NodeDataState{
			NodeID:     nodeID,
			LastUpdate: time.Now().Add(-time.Hour),
		}
	}
	c.nodeStatesMu.Unlock()

	// 根据配置的并发量，启动数据处理
	for i := 0; i < c.config.WorkerCount; i++ {
		c.wg.Add(1)
		go c.subWorker(i)
	}

	c.wg.Add(1)
	go c.heartbeatWorker()

	c.wg.Add(1)
	go c.staleCheckWorker()

	c.wg.Add(1)
	go c.publishWorker()

	topic := c.config.SubscriptionTopic
	if topic == "" {
		topic = "opcua/data"
	}

	handler := func(points []model.DataPoint) {
		select {
		case c.subCh <- points:
		default:
			c.logger.Warn("Subscription channel full, dropping batch",
				zap.Int("batch_size", len(points)))
		}
	}

	if err := c.opcuaClient.Subscribe(c.ctx, resolvedNodes, topic, handler); err != nil {
		return err
	}

	c.logger.Info("Collector started successfully")

	go c.monitorStats()

	return nil
}

// subWorker 订阅数据处理worker
func (c *Collector) subWorker(id int) {
	defer c.wg.Done()

	c.logger.Debug("Subscription worker started", zap.Int("worker_id", id))

	for {
		select {
		case points, ok := <-c.subCh:
			if !ok {
				c.logger.Debug("Subscription worker stopped", zap.Int("worker_id", id))
				return
			}

			for i := range points {
				c.updateNodeState(&points[i])
				c.pubCh <- points[i]
			}

		case <-c.ctx.Done():
			c.logger.Debug("Subscription worker stopped", zap.Int("worker_id", id))
			return
		}
	}
}

// heartbeatWorker 心跳验证worker - 定期拉取数据进行验证
func (c *Collector) heartbeatWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	c.logger.Info("Heartbeat worker started",
		zap.Int("interval_sec", c.config.HeartbeatIntervalSec))

	for {
		select {
		// 轮循定时
		case <-ticker.C:
			c.performHeartbeat()

		case <-c.ctx.Done():
			c.logger.Debug("Heartbeat worker stopped")
			return
		}
	}
}

// performHeartbeat 执行心跳验证，验证成功后更新LastUpdate，避免stale检测误报
func (c *Collector) performHeartbeat() {
	c.nodeStatesMu.RLock()
	nodeIDs := make([]string, 0, len(c.nodeStates))
	for nodeID := range c.nodeStates {
		nodeIDs = append(nodeIDs, nodeID)
	}
	c.nodeStatesMu.RUnlock()

	if len(nodeIDs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	readPoints, err := c.opcuaClient.ReadAll(ctx, nodeIDs)
	if err != nil {
		c.logger.Error("Heartbeat read failed", zap.Error(err))
		return
	}

	c.nodeStatesMu.Lock()
	defer c.nodeStatesMu.Unlock()

	for i := range readPoints {
		point := &readPoints[i]
		state, exists := c.nodeStates[point.NodeID]
		if !exists {
			continue
		}

		state.ReadValue = point.Value
		state.ReadTimestamp = point.Timestamp

		if point.Quality == "Good" {
			state.LastUpdate = time.Now()
			c.logger.Debug("Heartbeat read quality good",
				zap.String("node_id", point.NodeID))
		} else {
			c.logger.Warn("Heartbeat read quality bad",
				zap.String("node_id", point.NodeID),
				zap.String("quality", point.Quality))
		}
	}
}

// staleCheckWorker 停滞检查worker - 检查数据是否停滞
func (c *Collector) staleCheckWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	threshold := time.Duration(c.config.StaleThresholdSec) * time.Second

	c.logger.Info("Stale check worker started",
		zap.Int("threshold_sec", c.config.StaleThresholdSec))

	for {
		select {
		case <-ticker.C:
			c.checkStaleNodes(threshold)

		case <-c.ctx.Done():
			c.logger.Debug("Stale check worker stopped")
			return
		}
	}
}

// checkStaleNodes 检查停滞节点并发布Stale状态
func (c *Collector) checkStaleNodes(threshold time.Duration) {
	c.nodeStatesMu.Lock()
	defer c.nodeStatesMu.Unlock()

	now := time.Now()
	topic := c.config.SubscriptionTopic
	if topic == "" {
		topic = "opcua/data"
	}

	for nodeID, state := range c.nodeStates {
		age := now.Sub(state.LastUpdate)
		if age > threshold {
			stalePoint := model.DataPoint{
				NodeID:    nodeID,
				Value:     nil,
				Quality:   "Stale",
				Timestamp: now,
				Topic:     topic,
			}

			select {
			case c.pubCh <- stalePoint:
				c.staleCount.Add(1)
				c.logger.Warn("Stale data published",
					zap.String("node_id", nodeID),
					zap.Duration("age", age))
			default:
				c.logger.Warn("Publish channel full, stale data dropped",
					zap.String("node_id", nodeID))
			}
		}
	}
}

// publishWorker 发布worker
func (c *Collector) publishWorker() {
	defer c.wg.Done()

	batch := make([]model.DataPoint, 0, c.config.BatchSize)
	ticker := time.NewTicker(time.Duration(c.config.PublishTimeoutMs) * time.Millisecond)
	defer ticker.Stop()

	c.logger.Debug("Publish worker started")

	for {
		select {
		case point, ok := <-c.pubCh:
			if !ok {
				c.flushBatch(batch)
				c.logger.Debug("Publish worker stopped")
				return
			}

			batch = append(batch, point)

			if len(batch) >= c.config.BatchSize {
				c.processBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				c.processBatch(batch)
				batch = batch[:0]
			}

		case <-c.ctx.Done():
			c.flushBatch(batch)
			c.logger.Debug("Publish worker stopped")
			return
		}
	}
}

// updateNodeState 更新节点状态
func (c *Collector) updateNodeState(point *model.DataPoint) {
	c.nodeStatesMu.Lock()
	defer c.nodeStatesMu.Unlock()

	state, exists := c.nodeStates[point.NodeID]
	if !exists {
		state = &model.NodeDataState{
			NodeID: point.NodeID,
		}
		c.nodeStates[point.NodeID] = state
	}

	state.SubValue = point.Value
	state.SubTimestamp = point.Timestamp
	state.LastUpdate = time.Now()
}

// processBatch 处理一批数据
func (c *Collector) processBatch(batch []model.DataPoint) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	ctx, cancel := context.WithTimeout(c.ctx, time.Duration(c.config.PublishTimeoutMs)*time.Millisecond)
	defer cancel()

	topic := c.config.SubscriptionTopic
	if topic == "" {
		topic = "opcua/data"
	}

	err := c.natsClient.PublishBatch(ctx, topic, batch)

	latency := time.Since(start).Milliseconds()

	if err != nil {
		c.failCount.Add(int64(len(batch)))
		c.logger.Error("Failed to publish batch",
			zap.Int("batch_size", len(batch)),
			zap.Int64("latency_ms", latency),
			zap.Error(err))
	} else {
		c.successCount.Add(int64(len(batch)))
		c.totalPoints.Add(int64(len(batch)))
		c.logger.Debug("Batch published",
			zap.Int("batch_size", len(batch)),
			zap.Int64("latency_ms", latency))
	}
}

// flushBatch 刷新剩余批次
func (c *Collector) flushBatch(batch []model.DataPoint) {
	if len(batch) > 0 {
		c.processBatch(batch)
	}
}

// monitorStats 监控统计信息
func (c *Collector) monitorStats() {
	ticker := time.NewTicker(time.Duration(c.config.MonitorIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			uptime := time.Since(c.startTime)
			success := c.successCount.Load()
			fail := c.failCount.Load()
			stale := c.staleCount.Load()
			total := c.totalPoints.Load()

			stats := &model.CollectorStats{
				TotalPoints:  total,
				SuccessCount: success,
				FailureCount: fail,
				StaleCount:   stale,
				AvgLatencyMs: 0,
			}

			if success > 0 {
				stats.AvgLatencyMs = float64(total) / float64(success)
				stats.LastSuccessTime = time.Now()
			}
			if fail > 0 {
				stats.LastFailureTime = time.Now()
			}

			c.stats.Store(stats)

			c.logger.Info("Collector stats",
				zap.Int64("total_points", total),
				zap.Int64("success", success),
				zap.Int64("fail", fail),
				zap.Int64("stale", stale),
				zap.Duration("uptime", uptime))

		case <-c.ctx.Done():
			return
		}
	}
}

// GetStats 获取统计信息
func (c *Collector) GetStats() *model.CollectorStats {
	stats := c.stats.Load()
	if stats != nil {
		return stats.(*model.CollectorStats)
	}
	return nil
}

// Stop 停止采集器
func (c *Collector) Stop() {
	c.logger.Info("Stopping collector...")
	// 通过context通知停止，这里会调用c.ctx.Done()
	c.cancel()

	close(c.subCh)
	close(c.pubCh)
	c.wg.Wait()

	c.logger.Info("Collector stopped")
}
