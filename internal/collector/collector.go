// Package collector implements the data collection engine with memory pool and dual push modes.
package collector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
	"go-opcua-connector/internal/opcua"

	"go.uber.org/zap"
)

// Collector 数据采集器
// 内存池 + 双模式推送架构：
// - 内存池：nodeStates 作为唯一数据源，存储节点值、质量、时间戳、状态
// - 即时模式：每次变化立即推送，同时支持心跳强制刷新
// - 定时模式：累积脏标记，定时批量推送全量快照
// - 健康检查：合并心跳验证和停滞检测，先验证再判断
type Collector struct {
	// config 采集器配置
	config *config.CollectorConfig
	// opcuaClient OPC UA客户端
	opcuaClient *opcua.Client
	// publisher 发布器，由配置决定是 NATS 还是 MQTT
	publisher Publisher
	// logger 日志记录器
	logger *zap.Logger

	// subCh 订阅数据输入通道（即时模式）或更新触发通道（定时模式）
	subCh chan []model.DataPoint
	// pubCh 发布数据输出通道
	pubCh chan model.DataPoint
	// wg 等待组，管理所有worker协程
	wg sync.WaitGroup
	// ctx 根上下文
	ctx context.Context
	// cancel 取消函数
	cancel context.CancelFunc

	// nodeStates 内存池，key为NodeID, value为NodeDataState对象的指针
	nodeStates map[string]*model.NodeDataState
	// nodeStatesMu 保护nodeStates的读写锁
	nodeStatesMu sync.RWMutex

	// droppedPoints 丢弃的点数统计
	droppedPoints atomic.Int64
	// totalLatencyMs 累计延迟，用于计算平均延迟
	totalLatencyMs atomic.Int64

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
	publisher Publisher,
	logger *zap.Logger,
) *Collector {
	ctx, cancel := context.WithCancel(context.Background())

	return &Collector{
		config:      cfg,
		opcuaClient: opcuaClient,
		publisher:   publisher,
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
	c.setDefaults()

	c.logger.Info("Starting collector",
		zap.Int("worker_count", c.config.WorkerCount),
		zap.String("push_mode", string(c.config.PushMode)),
		zap.Int("channel_buffer", c.config.ChannelBufferSize),
		zap.Int("stale_threshold_sec", c.config.StaleThresholdSec),
		zap.Int("heartbeat_interval_sec", c.config.HeartbeatIntervalSec))

	if c.config.PushMode == config.PushModeTimed {
		c.logger.Info("Timed push mode config",
			zap.Int("push_interval_sec", c.config.PushIntervalSec))
	} else {
		c.logger.Info("Immediate push mode config",
			zap.Bool("force_heartbeat", c.config.ForceHeartbeat))
	}

	resolvedNodes, err := c.opcuaClient.ResolveNodes(c.ctx, c.config.SubscriptionNodes)
	if err != nil {
		return fmt.Errorf("failed to resolve nodes: %w", err)
	}

	c.logger.Info("Nodes resolved",
		zap.Int("configured_count", len(c.config.SubscriptionNodes)),
		zap.Int("resolved_count", len(resolvedNodes)))

	// 初始化节点状态
	c.initNodeStates(resolvedNodes)

	topic := c.config.SubscriptionTopic
	if topic == "" {
		topic = "opcua/data"
	}

	handler := func(points []model.DataPoint) {
		select {
		case c.subCh <- points:
		default:
			c.droppedPoints.Add(int64(len(points)))
			c.logger.Warn("Subscription channel full, dropping batch",
				zap.Int("batch_size", len(points)))
		}
	}

	if err := c.opcuaClient.Subscribe(c.ctx, resolvedNodes, topic, handler); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.monitorStats()

	for i := 0; i < c.config.WorkerCount; i++ {
		c.wg.Add(1)
		go c.subWorker(i)
	}

	c.wg.Add(1)
	go c.healthCheckWorker()

	c.wg.Add(1)
	go c.publishWorker()

	c.logger.Info("Collector started successfully")

	return nil
}

// setDefaults 设置配置默认值，防止零值导致panic
func (c *Collector) setDefaults() {
	if c.config.HeartbeatIntervalSec <= 0 {
		c.config.HeartbeatIntervalSec = 5
	}
	if c.config.StaleThresholdSec <= 0 {
		c.config.StaleThresholdSec = 30
	}
	if c.config.PushIntervalSec <= 0 {
		c.config.PushIntervalSec = 1
	}
	if c.config.WorkerCount <= 0 {
		c.config.WorkerCount = 10
	}
	if c.config.BatchSize <= 0 {
		c.config.BatchSize = 100
	}
	if c.config.ChannelBufferSize <= 0 {
		c.config.ChannelBufferSize = 10000
	}
	if c.config.PublishTimeoutMs <= 0 {
		c.config.PublishTimeoutMs = 5000
	}
	if c.config.MonitorIntervalSec <= 0 {
		c.config.MonitorIntervalSec = 60
	}
	if c.config.SubscriptionTopic == "" {
		c.config.SubscriptionTopic = "opcua/data"
	}
}

// initNodeStates 初始化内存池
func (c *Collector) initNodeStates(nodeIDs []string) {
	c.nodeStatesMu.Lock()
	defer c.nodeStatesMu.Unlock()

	for _, nodeID := range nodeIDs {
		c.nodeStates[nodeID] = &model.NodeDataState{
			NodeID:    nodeID,
			Timestamp: time.Now(),
			Status:    "Online",
		}
	}
}

// GetNodeDataType 获取指定节点的 OPC UA 数据类型
func (c *Collector) GetNodeDataType(nodeID string) string {
	c.nodeStatesMu.RLock()
	defer c.nodeStatesMu.RUnlock()

	if state, exists := c.nodeStates[nodeID]; exists {
		return state.DataType
	}
	return ""
}

// GetAllNodeDataTypes 获取所有节点的数据类型映射
func (c *Collector) GetAllNodeDataTypes() map[string]string {
	c.nodeStatesMu.RLock()
	defer c.nodeStatesMu.RUnlock()

	result := make(map[string]string, len(c.nodeStates))
	for nodeID, state := range c.nodeStates {
		result[nodeID] = state.DataType
	}
	return result
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
				c.handleDataPoint(&points[i])
			}

		case <-c.ctx.Done():
			c.logger.Debug("Subscription worker stopped", zap.Int("worker_id", id))
			return
		}
	}
}

// handleDataPoint 处理单个数据点，更新内存池并根据模式决定推送
func (c *Collector) handleDataPoint(point *model.DataPoint) {
	if strings.HasPrefix(point.Quality, "Bad") && point.Value == nil {
		return
	}

	c.nodeStatesMu.Lock()

	state, exists := c.nodeStates[point.NodeID]
	if !exists {
		state = &model.NodeDataState{
			NodeID: point.NodeID,
		}
		c.nodeStates[point.NodeID] = state
	}

	state.Value = point.Value
	state.Quality = point.Quality
	state.Timestamp = point.Timestamp
	state.Status = "Online"

	// 从订阅数据中更新实际 Go 类型（最准确）
	valueGoType := fmt.Sprintf("%T", point.Value)
	if state.DataType != valueGoType {
		c.logger.Info("Node value type updated from subscription",
			zap.String("node_id", point.NodeID),
			zap.String("old_type", state.DataType),
			zap.String("new_go_type", valueGoType),
			zap.Any("value", point.Value))
		state.DataType = valueGoType
	}

	// timed 模式：定时器会推送全量数据
	// immediate 模式：立即发送当前数据点
	if c.config.PushMode == config.PushModeImmediate {
		pointCopy := *point
		c.nodeStatesMu.Unlock()
		// 非阻塞发送，防止 OPC UA 回调线程被阻塞
		select {
		case c.pubCh <- pointCopy:
		default:
			c.droppedPoints.Add(1)
			c.logger.Warn("Publish channel full, dropping point",
				zap.String("node_id", point.NodeID))
		}
	} else {
		c.nodeStatesMu.Unlock()
	}
}

// healthCheckWorker 健康检查worker（合并心跳验证和停滞检测）
// - 心跳验证：主动ReadAll确认节点可达，防止稳态数据被误判为停滞
// - 停滞检测：检查LastUpdate超时，统一更新内存池状态
func (c *Collector) healthCheckWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()

	threshold := time.Duration(c.config.StaleThresholdSec) * time.Second

	c.logger.Info("Health check worker started",
		zap.Int("interval_sec", c.config.HeartbeatIntervalSec),
		zap.Int("stale_threshold_sec", c.config.StaleThresholdSec))

	for {
		select {
		case <-ticker.C:
			c.performHealthCheck(threshold)

		case <-c.ctx.Done():
			c.logger.Debug("Health check worker stopped")
			return
		}
	}
}

// performHealthCheck 分批读取所有节点值做心跳验证，防止单次 Read 节点数过多超时
func (c *Collector) performHealthCheck(threshold time.Duration) {
	c.nodeStatesMu.RLock()
	nodeIDs := make([]string, 0, len(c.nodeStates))
	for nodeID := range c.nodeStates {
		nodeIDs = append(nodeIDs, nodeID)
	}
	c.nodeStatesMu.RUnlock()

	if len(nodeIDs) == 0 {
		return
	}

	const healthCheckBatchSize = 500
	allPoints := make([]model.DataPoint, 0, len(nodeIDs))

	for i := 0; i < len(nodeIDs); i += healthCheckBatchSize {
		end := i + healthCheckBatchSize
		if end > len(nodeIDs) {
			end = len(nodeIDs)
		}
		batch := nodeIDs[i:end]

		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		readPoints, err := c.opcuaClient.ReadAll(ctx, batch)
		cancel()

		if err != nil {
			c.logger.Warn("Health check read batch failed",
				zap.Int("batch_start", i),
				zap.Int("batch_end", end),
				zap.Error(err))
			continue
		}
		allPoints = append(allPoints, readPoints...)
	}

	// 处理心跳结果
	for i := range allPoints {
		point := &allPoints[i]
		c.handleHeartbeatResult(point)
	}

	// 停滞检测
	c.checkStaleNodes(threshold)
}

// handleHeartbeatResult 处理心跳读取结果，更新内存池
func (c *Collector) handleHeartbeatResult(point *model.DataPoint) {
	c.nodeStatesMu.Lock()

	state, exists := c.nodeStates[point.NodeID]
	if !exists {
		c.nodeStatesMu.Unlock()
		return
	}

	if point.Quality == "Good" {
		state.Value = point.Value
		state.Quality = point.Quality
		state.Timestamp = point.Timestamp
		state.Status = "Online"

		if c.config.PushMode == config.PushModeImmediate && c.config.ForceHeartbeat {
			pointCopy := model.DataPoint{
				NodeID:    state.NodeID,
				Value:     state.Value,
				Quality:   state.Quality,
				Timestamp: state.Timestamp,
			}
			c.nodeStatesMu.Unlock()
			c.pubCh <- pointCopy
			return
		}

		c.logger.Debug("Heartbeat success",
			zap.String("node_id", point.NodeID),
			zap.String("quality", point.Quality))
	} else {
		state.Quality = point.Quality
		state.Timestamp = point.Timestamp
		c.logger.Warn("Heartbeat quality bad",
			zap.String("node_id", point.NodeID),
			zap.String("quality", point.Quality))
	}
	c.nodeStatesMu.Unlock()
}

// checkStaleNodes 检查停滞节点并更新内存池状态
func (c *Collector) checkStaleNodes(threshold time.Duration) {
	c.nodeStatesMu.Lock()

	now := time.Now()
	var toPublish []model.DataPoint

	for nodeID, state := range c.nodeStates {
		age := now.Sub(state.Timestamp)
		if age > threshold {
			if state.Status != "Stale" {
				state.Status = "Stale"
				c.staleCount.Add(1)

				c.logger.Warn("Node marked stale",
					zap.String("node_id", nodeID),
					zap.Duration("age", age))

				if c.config.PushMode == config.PushModeImmediate {
					toPublish = append(toPublish, model.DataPoint{
						NodeID:    nodeID,
						Value:     state.Value,
						Quality:   "Stale",
						Timestamp: now,
					})
				} else {
					state.Dirty = true
				}
			}
		}
	}
	c.nodeStatesMu.Unlock()

	for _, point := range toPublish {
		c.pubCh <- point
	}
}

// getTopic 获取当前配置的发布主题
func (c *Collector) getTopic() string {
	topic := c.config.SubscriptionTopic
	if topic == "" {
		return "opcua/data"
	}
	return topic
}

// publishWorker 发布worker，支持即时和定时双模式
func (c *Collector) publishWorker() {
	defer c.wg.Done()

	if c.config.PushMode == config.PushModeTimed {
		c.publishWorkerTimed()
	} else {
		c.publishWorkerImmediate()
	}
}

// publishWorkerImmediate 即时模式：直接转发数据点
func (c *Collector) publishWorkerImmediate() {
	c.logger.Info("Publish worker started (immediate mode)")

	for {
		select {
		case point, ok := <-c.pubCh:
			if !ok {
				c.logger.Debug("Publish worker stopped")
				return
			}
			c.processBatch([]model.DataPoint{point})

		case <-c.ctx.Done():
			c.logger.Debug("Publish worker stopped")
			return
		}
	}
}

// publishWorkerTimed 定时模式：定时批量推送所有脏节点的全量快照
func (c *Collector) publishWorkerTimed() {
	ticker := time.NewTicker(time.Duration(c.config.PushIntervalSec) * time.Second)
	defer ticker.Stop()

	c.logger.Info("Publish worker started (timed mode)",
		zap.Int("push_interval_sec", c.config.PushIntervalSec))

	for {
		select {
		case <-ticker.C:
			c.pushDirtyNodes()

		case <-c.ctx.Done():
			c.logger.Debug("Publish worker stopped")
			return
		}
	}
}

// pushDirtyNodes 推送所有节点的全量快照（定时模式：固定周期推送）
func (c *Collector) pushDirtyNodes() {
	c.nodeStatesMu.Lock()
	batch := make([]model.DataPoint, 0, len(c.nodeStates))

	for _, state := range c.nodeStates {
		if strings.HasPrefix(state.Quality, "Bad") && state.Value == nil {
			continue
		}
		// 无论 Dirty 是否为 true，都推送全量数据
		// 这是固定周期推送的核心：保证下游持续收到数据
		batch = append(batch, model.DataPoint{
			NodeID:    state.NodeID,
			Value:     state.Value,
			Quality:   state.Quality,
			Timestamp: state.Timestamp,
		})
	}
	c.nodeStatesMu.Unlock()

	if len(batch) > 0 {
		c.processBatch(batch)
	}
}

// processBatch 处理一批数据
func (c *Collector) processBatch(batch []model.DataPoint) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	ctx, cancel := context.WithTimeout(c.ctx, time.Duration(c.config.PublishTimeoutMs)*time.Millisecond)
	defer cancel()

	err := c.publisher.PublishBatch(ctx, c.getTopic(), batch)

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
		c.totalLatencyMs.Add(latency)
		c.logger.Debug("Batch published",
			zap.Int("batch_size", len(batch)),
			zap.Int64("latency_ms", latency))
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
			totalLatency := c.totalLatencyMs.Load()
			dropped := c.droppedPoints.Load()

			stats := &model.CollectorStats{
				TotalPoints:   total,
				SuccessCount:  success,
				FailureCount:  fail,
				StaleCount:    stale,
				DroppedPoints: dropped,
				AvgLatencyMs:  0,
			}

			if success > 0 {
				stats.AvgLatencyMs = float64(totalLatency) / float64(success)
			}

			c.stats.Store(stats)

			c.logger.Info("Collector stats",
				zap.Int64("total_points", total),
				zap.Int64("success", success),
				zap.Int64("fail", fail),
				zap.Int64("stale", stale),
				zap.Int64("dropped", dropped),
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
	c.cancel()

	c.wg.Wait()

	close(c.subCh)
	close(c.pubCh)

	c.logger.Info("Collector stopped")
}
