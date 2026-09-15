package collector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"

	"go.uber.org/zap"
)

// Collector 负责组装采集协调器，并将 StateStore 快照适配到现有发送接口。
type Collector struct {
	config    *config.CollectorConfig
	adapter   SourceAdapter
	store     StateStore
	publisher Publisher
	logger    *zap.Logger

	lifecycleMu sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	acquisition *AcquisitionCoordinator
	started     bool
	stopped     bool
	wg          sync.WaitGroup

	totalLatencyMs atomic.Int64
	totalPoints    atomic.Int64
	successCount   atomic.Int64
	failCount      atomic.Int64
	stats          atomic.Value
	startTime      time.Time
}

type publishedPointState struct {
	version            uint64
	health             PointHealth
	lastSubscriptionAt time.Time
}

// New 创建使用 StateStore 新采集链路的采集器门面。
func New(cfg *config.CollectorConfig, adapter SourceAdapter, publisher Publisher, logger *zap.Logger) *Collector {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Collector{
		config:    cfg,
		adapter:   adapter,
		store:     NewStateStore(),
		publisher: publisher,
		logger:    logger,
		startTime: time.Now(),
	}
}

// Start 启动采集协调器和发送协程。
func (c *Collector) Start() error {
	c.lifecycleMu.Lock()
	if c.started {
		c.lifecycleMu.Unlock()
		return errors.New("collector already started")
	}
	if c.config == nil {
		c.lifecycleMu.Unlock()
		return errors.New("collector config is required")
	}
	if c.publisher == nil {
		c.lifecycleMu.Unlock()
		return errors.New("collector publisher is required")
	}
	if c.config.PublishTimeoutMs <= 0 || c.config.MonitorIntervalSec <= 0 ||
		c.config.PushMode == config.PushModeTimed && c.config.PushIntervalSec <= 0 {
		c.lifecycleMu.Unlock()
		return errors.New("collector publishing intervals and timeout must be positive")
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.started = true
	c.lifecycleMu.Unlock()

	acquisition, err := NewAcquisitionCoordinator(c.adapter, c.store, *c.config, c.logger)
	if err != nil {
		c.failStart()
		return err
	}
	c.acquisition = acquisition
	if err := acquisition.Start(c.ctx); err != nil {
		c.failStart()
		return err
	}

	if c.config.WorkerCount > 0 || c.config.ChannelBufferSize > 0 {
		c.logger.Info("collector worker_count and channel_buffer_size are deprecated by the StateStore acquisition path",
			zap.Int("worker_count", c.config.WorkerCount),
			zap.Int("channel_buffer_size", c.config.ChannelBufferSize))
	}
	c.wg.Add(2)
	go c.publishLoop()
	go c.monitorStats()
	c.logger.Info("Collector started successfully", zap.String("push_mode", string(c.config.PushMode)))
	return nil
}

func (c *Collector) failStart() {
	c.lifecycleMu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	c.stopped = true
	c.lifecycleMu.Unlock()
}

func (c *Collector) publishLoop() {
	defer c.wg.Done()
	if c.config.PushMode == config.PushModeImmediate {
		c.publishImmediate()
		return
	}
	c.publishTimed()
}

func (c *Collector) publishImmediate() {
	known := make(map[PointID]publishedPointState)
	changes := c.store.SubscribeChanges()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-changes:
			c.publishBatches(c.changedDataPoints(known))
		}
	}
}

func (c *Collector) publishTimed() {
	ticker := time.NewTicker(time.Duration(c.config.PushIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.publishCurrentSnapshot()
		}
	}
}

func (c *Collector) changedDataPoints(known map[PointID]publishedPointState) []model.DataPoint {
	snapshots := c.store.Snapshot()
	current := make(map[PointID]struct{}, len(snapshots))
	points := make([]model.DataPoint, 0, len(snapshots))
	for _, snapshot := range snapshots {
		current[snapshot.PointID] = struct{}{}
		previous, exists := known[snapshot.PointID]
		known[snapshot.PointID] = publishedPointState{
			version: snapshot.Version, health: snapshot.Health, lastSubscriptionAt: snapshot.LastSubscriptionAt,
		}
		if exists && previous.version == snapshot.Version {
			continue
		}
		shouldPublish := !exists || c.config.ForceHeartbeat ||
			!snapshot.LastSubscriptionAt.Equal(previous.lastSubscriptionAt) || snapshot.Health != previous.health
		if shouldPublish {
			c.appendDataPoint(&points, snapshot)
		}
	}
	for pointID := range known {
		if _, exists := current[pointID]; !exists {
			delete(known, pointID)
		}
	}
	return points
}

func (c *Collector) publishCurrentSnapshot() {
	snapshots := c.store.Snapshot()
	points := make([]model.DataPoint, 0, len(snapshots))
	for _, snapshot := range snapshots {
		c.appendDataPoint(&points, snapshot)
	}
	c.publishBatches(points)
}

func (c *Collector) appendDataPoint(points *[]model.DataPoint, snapshot PointSnapshot) {
	point := dataPointFromSnapshot(snapshot)
	if c.config.FilterBadQuality && point.Quality != "Good" {
		return
	}
	*points = append(*points, point)
}

func dataPointFromSnapshot(snapshot PointSnapshot) model.DataPoint {
	quality := string(snapshot.Health)
	if snapshot.Health == PointHealthy {
		quality = "Good"
	}
	timestamp := snapshot.ObservedAt
	if timestamp.IsZero() {
		timestamp = snapshot.LastStateChangedAt
	}
	return model.DataPoint{
		NodeID:    string(snapshot.PointID),
		Value:     snapshot.Value,
		Quality:   quality,
		Timestamp: timestamp,
	}
}

func (c *Collector) publishBatches(points []model.DataPoint) {
	if len(points) == 0 {
		return
	}
	batchSize := c.config.BatchSize
	if batchSize <= 0 || batchSize >= len(points) {
		c.processBatch(points)
		return
	}
	for start := 0; start < len(points); start += batchSize {
		end := start + batchSize
		if end > len(points) {
			end = len(points)
		}
		c.processBatch(points[start:end])
	}
}

func (c *Collector) processBatch(batch []model.DataPoint) {
	if len(batch) == 0 || c.ctx.Err() != nil {
		return
	}
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, time.Duration(c.config.PublishTimeoutMs)*time.Millisecond)
	err := c.publisher.PublishBatch(ctx, c.topic(), batch)
	cancel()
	latency := time.Since(startedAt).Milliseconds()
	if err != nil {
		c.failCount.Add(int64(len(batch)))
		c.logger.Error("Failed to publish batch", zap.Int("batch_size", len(batch)), zap.Error(err))
		return
	}
	c.successCount.Add(int64(len(batch)))
	c.totalPoints.Add(int64(len(batch)))
	c.totalLatencyMs.Add(latency)
}

func (c *Collector) topic() string {
	if c.config.SubscriptionTopic == "" {
		return "opcua/data"
	}
	return c.config.SubscriptionTopic
}

func (c *Collector) monitorStats() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(c.config.MonitorIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			stats := c.collectStats()
			c.stats.Store(stats)
			fields := pointHealthLogFields(*stats)
			fields = append(fields,
				zap.Int64("published_points", stats.SuccessCount),
				zap.Int64("failed_points", stats.FailureCount),
				zap.Duration("uptime", time.Since(c.startTime)))
			c.logger.Info("Collector stats", fields...)
		}
	}
}

func (c *Collector) collectStats() *model.CollectorStats {
	success := c.successCount.Load()
	snapshots := c.store.Snapshot()
	stats := summarizePointHealth(snapshots)
	stats.TotalPoints = c.totalPoints.Load()
	stats.SuccessCount = success
	stats.FailureCount = c.failCount.Load()
	if success > 0 {
		stats.AvgLatencyMs = float64(c.totalLatencyMs.Load()) / float64(success)
	}
	return &stats
}

// GetStats 获取最近一次统计快照；首次统计周期前按当前计数生成。
func (c *Collector) GetStats() *model.CollectorStats {
	if stats := c.stats.Load(); stats != nil {
		return stats.(*model.CollectorStats)
	}
	return c.collectStats()
}

// Stop 幂等停止采集和发送协程。
func (c *Collector) Stop() {
	c.lifecycleMu.Lock()
	if !c.started || c.stopped {
		c.lifecycleMu.Unlock()
		return
	}
	c.stopped = true
	c.cancel()
	acquisition := c.acquisition
	c.lifecycleMu.Unlock()

	stopCtx, cancel := context.WithTimeout(context.Background(), defaultAcquisitionStopTimeout)
	defer cancel()
	if acquisition != nil {
		if err := acquisition.Stop(stopCtx); err != nil {
			c.logger.Warn("Stop acquisition coordinator failed", zap.Error(err))
		}
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-stopCtx.Done():
		c.logger.Warn("Stop collector workers timed out", zap.Error(stopCtx.Err()))
	}
	c.logger.Info("Collector stopped")
}
