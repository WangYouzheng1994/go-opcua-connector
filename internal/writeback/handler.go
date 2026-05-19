package writeback

import (
	"context"
	"encoding/json"
	"fmt"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
	natsclient "go-opcua-connector/internal/nats"
	"go-opcua-connector/internal/opcua"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// Handler 回写处理器，订阅NATS回写命令、类型转换、执行OPC UA写操作、发布结果
type Handler struct {
	// opcuaClient OPC UA客户端
	opcuaClient *opcua.Client
	// natsClient NATS客户端，用于订阅命令和发布结果
	natsClient *natsclient.Client
	// logger 日志记录器
	logger *zap.Logger
	// sub NATS订阅实例
	sub *nats.Subscription
	// writeSubject 接收回写命令的NATS主题
	writeSubject string
	// resultSubject 发布回写结果的NATS主题
	resultSubject string
}

// NewHandler 创建回写处理器
func NewHandler(opcuaClient *opcua.Client, natsClient *natsclient.Client, cfg *config.WritebackConfig, logger *zap.Logger) *Handler {
	return &Handler{
		opcuaClient:   opcuaClient,
		natsClient:    natsClient,
		logger:        logger,
		writeSubject:  cfg.WriteSubject,
		resultSubject: cfg.ResultSubject,
	}
}

// Start 订阅回写命令NATS主题并开始处理
func (h *Handler) Start(ctx context.Context) error {
	sub, err := h.natsClient.Subscribe(ctx, h.writeSubject, h.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to writeback subject %s: %w", h.writeSubject, err)
	}

	h.sub = sub
	h.logger.Info("Writeback handler started",
		zap.String("write_subject", h.writeSubject),
		zap.String("result_subject", h.resultSubject))
	return nil
}

// Stop 停止回写处理器
func (h *Handler) Stop() {
	if h.sub != nil {
		h.sub.Unsubscribe()
		h.sub = nil
	}
	h.logger.Info("Writeback handler stopped")
}

// handleMessage 处理NATS回写命令消息
// 解析JSON → 类型转换 → 写入OPC UA → 发布结果
func (h *Handler) handleMessage(msg *nats.Msg) {
	var cmd model.WriteCommand
	if err := json.Unmarshal(msg.Data, &cmd); err != nil {
		h.logger.Error("Failed to parse write command", zap.Error(err))
		h.publishResult(model.WriteResult{
			Error: fmt.Sprintf("invalid JSON: %v", err),
		})
		return
	}

	if cmd.NodeID == "" {
		h.publishResult(model.WriteResult{
			RequestID: cmd.RequestID,
			Error:     "node_id is required",
		})
		return
	}

	convertedValue, err := opcua.ConvertWriteValue(cmd.Value, cmd.ValueType)
	if err != nil {
		h.logger.Warn("Value conversion failed",
			zap.String("node_id", cmd.NodeID),
			zap.String("raw_value", cmd.Value),
			zap.String("value_type", cmd.ValueType),
			zap.Error(err))
		h.publishResult(model.WriteResult{
			NodeID:    cmd.NodeID,
			RequestID: cmd.RequestID,
			Error:     err.Error(),
		})
		return
	}

	if err := h.opcuaClient.Write(context.Background(), cmd.NodeID, convertedValue); err != nil {
		h.logger.Error("Write failed",
			zap.String("node_id", cmd.NodeID),
			zap.Any("converted_value", convertedValue),
			zap.Error(err))
		h.publishResult(model.WriteResult{
			NodeID:    cmd.NodeID,
			RequestID: cmd.RequestID,
			Error:     err.Error(),
		})
		return
	}

	h.logger.Info("Write succeeded",
		zap.String("node_id", cmd.NodeID),
		zap.Any("value", convertedValue))

	h.publishResult(model.WriteResult{
		NodeID:       cmd.NodeID,
		RequestID:    cmd.RequestID,
		Success:      true,
		WrittenValue: convertedValue,
	})
}

// publishResult 发布回写结果到NATS
func (h *Handler) publishResult(result model.WriteResult) {
	data, err := json.Marshal(result)
	if err != nil {
		h.logger.Error("Failed to marshal write result", zap.Error(err))
		return
	}

	if err := h.natsClient.PublishRaw(h.resultSubject, data); err != nil {
		h.logger.Error("Failed to publish write result", zap.Error(err))
	}
}
