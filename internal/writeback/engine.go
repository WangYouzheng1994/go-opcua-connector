// Package writeback implements the writeback engine for OPC UA write commands.
package writeback

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/opcua"

	"go.uber.org/zap"
)

const (
	shutdownGracePeriod  = 10 * time.Second
	resultPublishTimeout = 5 * time.Second
)

// Engine 负责统一 PointID 请求的订阅、入队、执行结果发布和有限优雅退出。
type Engine struct {
	transport       Transport
	logger          *zap.Logger
	sub             Subscription
	writeSubject    string
	resultSubject   string
	executor        *QueueExecutor
	shutdownGrace   time.Duration
	stopOnce        sync.Once
	lifecycleMu     sync.RWMutex
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	publishEnabled  atomic.Bool
}

// NewEngine 创建以统一 PointID Request 为内部模型的回写引擎。
func NewEngine(opcuaClient *opcua.Client, transport Transport, cfg *config.WritebackConfig, logger *zap.Logger) *Engine {
	engine := &Engine{
		transport:     transport,
		logger:        logger,
		writeSubject:  cfg.WriteSubject,
		resultSubject: cfg.ResultSubject,
		shutdownGrace: shutdownGracePeriod,
		lifecycleCtx:  context.Background(),
	}
	engine.publishEnabled.Store(true)
	engine.executor = NewQueueExecutor(opcuaClient, engine.publishResult)
	return engine
}

// Start 订阅回写命令并启动唯一 Worker。
func (e *Engine) Start(ctx context.Context) error {
	sub, err := e.transport.Subscribe(ctx, e.writeSubject, e.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to writeback subject %s: %w", e.writeSubject, err)
	}

	e.sub = sub
	runCtx, runCancel := context.WithCancel(ctx)
	e.lifecycleMu.Lock()
	e.lifecycleCtx = runCtx
	e.lifecycleCancel = runCancel
	e.lifecycleMu.Unlock()
	go e.executor.Run(runCtx)
	e.logger.Info("Writeback engine started",
		zap.String("write_subject", e.writeSubject),
		zap.String("result_subject", e.resultSubject))
	return nil
}

// Stop 停止接收新请求，并给予当前请求固定的有限优雅退出时间。
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		e.executor.StopAccepting()
		if e.sub != nil {
			if err := e.sub.Unsubscribe(); err != nil {
				e.logger.Warn("Failed to unsubscribe writeback subject", zap.Error(err))
			}
			e.sub = nil
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), e.shutdownGrace)
		err := e.executor.Shutdown(shutdownCtx)
		cancel()
		if err != nil {
			e.logger.Warn("Writeback shutdown grace period expired",
				zap.Duration("grace_period", e.shutdownGrace),
				zap.Error(err))
		}

		e.lifecycleMu.RLock()
		cancelLifecycle := e.lifecycleCancel
		e.lifecycleMu.RUnlock()
		if cancelLifecycle != nil {
			cancelLifecycle()
		}
		e.publishEnabled.Store(false)
		e.logger.Info("Writeback engine stopped")
	})
}

// handleMessage 只负责协议适配、严格校验和非阻塞入队，不执行 OPC UA I/O。
func (e *Engine) handleMessage(data []byte) {
	request, err := decodeIncomingRequest(data)
	if err != nil {
		e.logger.Warn("Rejected invalid write request", zap.Error(err))
		e.publishResult(NewRejectedResult(
			requestIDFromPayload(data),
			ErrorCodeInvalidRequest,
			err.Error(),
		))
		return
	}

	if err := e.executor.TrySubmit(request); err != nil {
		if code, ok := ErrorCodeOf(err); ok && code == ErrorCodeWriteQueueFull {
			e.publishResult(NewRejectedResult(request.RequestID, code, "write request queue is full"))
			return
		}
		e.logger.Debug("Write request ignored during shutdown", zap.String("request_id", request.RequestID))
	}
}

func (e *Engine) publishResult(result Result) {
	if !e.publishEnabled.Load() {
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		e.logger.Error("Failed to marshal write result", zap.Error(err))
		return
	}

	e.lifecycleMu.RLock()
	baseCtx := e.lifecycleCtx
	e.lifecycleMu.RUnlock()
	ctx, cancel := context.WithTimeout(baseCtx, resultPublishTimeout)
	defer cancel()

	if err := e.transport.PublishResult(ctx, e.resultSubject, data); err != nil {
		e.logger.Error("Failed to publish write result", zap.Error(err))
	}
}
