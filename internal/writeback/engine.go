// Package writeback implements the writeback engine for OPC UA write commands.
package writeback

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"
	"go-opcua-connector/internal/opcua"

	"go.uber.org/zap"
)

// Engine 回写引擎。
// 负责：订阅命令 → JSON解析 → 类型转换 → OPC UA写入 → 发布结果。
// 通过 Transport 接口与消息中间件解耦。
type Engine struct {
	opcuaClient     *opcua.Client
	transport       Transport
	logger          *zap.Logger
	sub             Subscription
	writeSubject    string
	resultSubject   string
	nodeIDPrefix    string
	nodeDataTypes   map[string]string
	nodeDataTypesMu sync.RWMutex
}

// NewEngine 创建回写引擎。
// nodeIDPrefix 用于还原短节点ID为完整OPC UA节点ID，如 "ns=2;s="。
func NewEngine(opcuaClient *opcua.Client, transport Transport, cfg *config.WritebackConfig, nodeIDPrefix string, logger *zap.Logger) *Engine {
	return &Engine{
		opcuaClient:   opcuaClient,
		transport:     transport,
		logger:        logger,
		writeSubject:  cfg.WriteSubject,
		resultSubject: cfg.ResultSubject,
		nodeIDPrefix:  nodeIDPrefix,
		nodeDataTypes: make(map[string]string),
	}
}

// Start 订阅回写命令并开始处理。
func (e *Engine) Start(ctx context.Context) error {
	sub, err := e.transport.Subscribe(ctx, e.writeSubject, e.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to writeback subject %s: %w", e.writeSubject, err)
	}

	e.sub = sub
	e.logger.Info("Writeback engine started",
		zap.String("write_subject", e.writeSubject),
		zap.String("result_subject", e.resultSubject))
	return nil
}

// SetNodeDataTypes 设置节点数据类型缓存，用于自动类型适配。
func (e *Engine) SetNodeDataTypes(dataTypes map[string]string) {
	e.nodeDataTypesMu.Lock()
	defer e.nodeDataTypesMu.Unlock()
	e.nodeDataTypes = dataTypes
	e.logger.Info("Node data types cached for writeback",
		zap.Int("count", len(dataTypes)))
}

func (e *Engine) getNodeDataType(nodeID string) string {
	e.nodeDataTypesMu.RLock()
	defer e.nodeDataTypesMu.RUnlock()
	if dt, ok := e.nodeDataTypes[nodeID]; ok {
		return dt
	}
	return ""
}

// Stop 停止回写引擎。
func (e *Engine) Stop() {
	if e.sub != nil {
		e.sub.Unsubscribe()
		e.sub = nil
	}
	e.logger.Info("Writeback engine stopped")
}

// handleMessage 处理回写命令消息，支持新格式 [{"id":"...","v":...}] 和旧格式 {"node_id":"..."}。
func (e *Engine) handleMessage(data []byte) {
	var items []model.BatchWriteItem
	if err := json.Unmarshal(data, &items); err == nil && len(items) > 0 {
		for _, item := range items {
			cmd := item.ToWriteCommand(e.nodeIDPrefix)
			e.processWriteCommand(&cmd)
		}
		return
	}

	var cmd model.WriteCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		e.logger.Error("Failed to parse write command", zap.Error(err))
		e.publishResult(model.WriteResult{
			Error: fmt.Sprintf("invalid JSON: %v", err),
		})
		return
	}

	e.processWriteCommand(&cmd)
}

func (e *Engine) processWriteCommand(cmd *model.WriteCommand) {
	if cmd.NodeID == "" {
		e.publishResult(model.WriteResult{
			RequestID: cmd.RequestID,
			Error:     "node_id is required",
		})
		return
	}

	metadataType := e.getNodeDataType(cmd.NodeID)
	userType := cmd.ValueType

	if metadataType == "" {
		readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer readCancel()
		dt, err := e.opcuaClient.ReadNodeDataTypes(readCtx, []string{cmd.NodeID})
		if err != nil {
			e.logger.Warn("Failed to lazy-read node data type",
				zap.String("node_id", cmd.NodeID), zap.Error(err))
		} else if t, ok := dt[cmd.NodeID]; ok && t != "" {
			metadataType = t
			e.nodeDataTypesMu.Lock()
			e.nodeDataTypes[cmd.NodeID] = t
			e.nodeDataTypesMu.Unlock()
			e.logger.Debug("Node data type lazy-loaded",
				zap.String("node_id", cmd.NodeID),
				zap.String("data_type", t))
		}
	}

	var writeValue any
	convertedType := ""
	var err error

	if userType != "" {
		if !isCompatibleTypeGroup(userType, metadataType) {
			e.logger.Warn("User type may be incompatible with node metadata type",
				zap.String("node_id", cmd.NodeID),
				zap.String("user_type", userType),
				zap.String("metadata_type", metadataType))
		}
		writeValue, err = opcua.ConvertWriteValue(cmd.Value, userType)
		if err != nil {
			e.logger.Warn("Value conversion with user type failed, retrying with metadata type",
				zap.String("node_id", cmd.NodeID),
				zap.String("raw_value", cmd.Value),
				zap.String("user_type", userType),
				zap.String("metadata_type", metadataType),
				zap.Error(err))
			if metadataType != "" && metadataType != userType {
				writeValue, err = opcua.ConvertWriteValue(cmd.Value, metadataType)
				if err != nil {
					e.logger.Warn("Value conversion with metadata type also failed, using raw string",
						zap.String("node_id", cmd.NodeID),
						zap.String("raw_value", cmd.Value),
						zap.String("metadata_type", metadataType),
						zap.Error(err))
					writeValue = cmd.Value
				} else {
					convertedType = metadataType
				}
			} else {
				writeValue = cmd.Value
			}
		} else {
			convertedType = userType
		}
	} else if metadataType != "" && metadataType != "String" {
		writeValue, err = opcua.ConvertWriteValue(cmd.Value, metadataType)
		if err != nil {
			e.logger.Warn("Value conversion with metadata type failed, using raw string",
				zap.String("node_id", cmd.NodeID),
				zap.String("raw_value", cmd.Value),
				zap.String("metadata_type", metadataType),
				zap.Error(err))
			writeValue = cmd.Value
		} else {
			convertedType = metadataType
		}
	} else {
		writeValue = cmd.Value
	}

	e.logger.Info("Write value prepared",
		zap.String("node_id", cmd.NodeID),
		zap.String("user_type", userType),
		zap.String("metadata_type", metadataType),
		zap.String("converted_type", convertedType),
		zap.Any("value", writeValue),
		zap.String("value_kind", fmt.Sprintf("%T", writeValue)))

	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	writeErr := e.opcuaClient.Write(writeCtx, cmd.NodeID, writeValue)
	if writeErr != nil {
		if userType != "" && metadataType != "" && !isCompatibleTypeGroup(userType, metadataType) {
			e.logger.Warn("Write failed with user type, retrying with metadata type",
				zap.String("node_id", cmd.NodeID),
				zap.String("user_type", userType),
				zap.String("metadata_type", metadataType),
				zap.Error(writeErr))
			retryValue, retryErr := opcua.ConvertWriteValue(cmd.Value, metadataType)
			if retryErr == nil {
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer retryCancel()
				var retryWriteErr error
				retryWriteErr = e.opcuaClient.Write(retryCtx, cmd.NodeID, retryValue)
				if retryWriteErr == nil {
					e.logger.Info("Write succeeded with metadata type retry",
						zap.String("node_id", cmd.NodeID),
						zap.String("metadata_type", metadataType),
						zap.Any("value", retryValue))
					e.publishResult(model.WriteResult{
						NodeID:       cmd.NodeID,
						RequestID:    cmd.RequestID,
						Success:      true,
						WrittenValue: retryValue,
					})
					return
				}
				e.logger.Error("Write retry with metadata type also failed",
					zap.String("node_id", cmd.NodeID),
					zap.Error(retryWriteErr))
			} else {
				e.logger.Warn("Metadata type conversion failed in retry",
					zap.String("node_id", cmd.NodeID),
					zap.Error(retryErr))
			}
		}
		e.logger.Error("Write failed",
			zap.String("node_id", cmd.NodeID),
			zap.Any("value", writeValue),
			zap.Error(writeErr))
		e.publishResult(model.WriteResult{
			NodeID:    cmd.NodeID,
			RequestID: cmd.RequestID,
			Error:     writeErr.Error(),
		})
		return
	}

	e.logger.Info("Write succeeded",
		zap.String("node_id", cmd.NodeID),
		zap.Any("value", writeValue))

	e.publishResult(model.WriteResult{
		NodeID:       cmd.NodeID,
		RequestID:    cmd.RequestID,
		Success:      true,
		WrittenValue: writeValue,
	})
}

func typeGroup(t string) int {
	switch strings.ToLower(t) {
	case "bool", "boolean":
		return 1
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"sbyte", "byte", "integer":
		return 2
	case "float32", "float64", "float", "double":
		return 3
	case "string":
		return 4
	}
	return 0
}

func isCompatibleTypeGroup(t1, t2 string) bool {
	if t2 == "" || t2 == "String" {
		return true
	}
	g1, g2 := typeGroup(t1), typeGroup(t2)
	if g1 == 0 || g2 == 0 {
		return true
	}
	return g1 == g2
}

func (e *Engine) publishResult(result model.WriteResult) {
	data, err := json.Marshal(result)
	if err != nil {
		e.logger.Error("Failed to marshal write result", zap.Error(err))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.transport.PublishResult(ctx, e.resultSubject, data); err != nil {
		e.logger.Error("Failed to publish write result", zap.Error(err))
	}
}
