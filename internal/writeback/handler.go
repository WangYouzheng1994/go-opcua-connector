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
	// nodeDataTypes 节点数据类型缓存，用于自动类型适配
	nodeDataTypes   map[string]string
	nodeDataTypesMu sync.RWMutex
}

// NewHandler 创建回写处理器
func NewHandler(opcuaClient *opcua.Client, natsClient *natsclient.Client, cfg *config.WritebackConfig, logger *zap.Logger) *Handler {
	return &Handler{
		opcuaClient:   opcuaClient,
		natsClient:    natsClient,
		logger:        logger,
		writeSubject:  cfg.WriteSubject,
		resultSubject: cfg.ResultSubject,
		nodeDataTypes: make(map[string]string),
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

// SetNodeDataTypes 设置节点数据类型缓存，用于自动类型适配
func (h *Handler) SetNodeDataTypes(dataTypes map[string]string) {
	h.nodeDataTypesMu.Lock()
	defer h.nodeDataTypesMu.Unlock()
	h.nodeDataTypes = dataTypes
	h.logger.Info("Node data types cached for writeback",
		zap.Int("count", len(dataTypes)))
}

// getNodeDataType 获取已缓存的节点数据类型
func (h *Handler) getNodeDataType(nodeID string) string {
	h.nodeDataTypesMu.RLock()
	defer h.nodeDataTypesMu.RUnlock()
	if dt, ok := h.nodeDataTypes[nodeID]; ok {
		return dt
	}
	return ""
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

	// 元数据类型：从 collector 订阅数据中实时检测的 Go 类型名（如 "int32", "float64", "bool"）
	metadataType := h.getNodeDataType(cmd.NodeID)
	// 用户指定类型：上游通过 JSON 字段 value_type 下发，优先级最高
	userType := cmd.ValueType

	// 惰性加载：元数据类型未缓存时，实时从 OPC UA 读取该节点的 DataType 属性
	if metadataType == "" {
		readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer readCancel()
		dt, err := h.opcuaClient.ReadNodeDataTypes(readCtx, []string{cmd.NodeID})
		if err != nil {
			h.logger.Warn("Failed to lazy-read node data type",
				zap.String("node_id", cmd.NodeID), zap.Error(err))
		} else if t, ok := dt[cmd.NodeID]; ok && t != "" {
			metadataType = t
			h.nodeDataTypesMu.Lock()
			h.nodeDataTypes[cmd.NodeID] = t
			h.nodeDataTypesMu.Unlock()
			h.logger.Debug("Node data type lazy-loaded",
				zap.String("node_id", cmd.NodeID),
				zap.String("data_type", t))
		}
	}

	var writeValue any
	convertedType := ""
	var err error

	// 转换策略：
	//   1. 用户指定类型 → 优先用用户类型转换，不兼容时先兼容性检测告警
	//   2. 用户类型转换失败 → 回退到元数据类型转换
	//   3. 用户未指定类型 → 以元数据类型为目标转换
	//   4. 所有转换都失败 → 兜底用原始字符串（可能被 KepServer 拒绝）
	if userType != "" {
		// 用户指定了类型：先做兼容性检测，跨族时告警但仍尝试写入
		if !isCompatibleTypeGroup(userType, metadataType) {
			h.logger.Warn("User type may be incompatible with node metadata type",
				zap.String("node_id", cmd.NodeID),
				zap.String("user_type", userType),
				zap.String("metadata_type", metadataType))
		}
		// 尝试按用户指定类型转换
		writeValue, err = opcua.ConvertWriteValue(cmd.Value, userType)
		if err != nil {
			// 用户类型转换失败（如 boolean 解析不了 "42"），回退到元数据类型
			h.logger.Warn("Value conversion with user type failed, retrying with metadata type",
				zap.String("node_id", cmd.NodeID),
				zap.String("raw_value", cmd.Value),
				zap.String("user_type", userType),
				zap.String("metadata_type", metadataType),
				zap.Error(err))
			if metadataType != "" && metadataType != userType {
				// 回退到元数据类型转换
				writeValue, err = opcua.ConvertWriteValue(cmd.Value, metadataType)
				if err != nil {
					// 元数据类型也转换失败，兜底原始字符串
					h.logger.Warn("Value conversion with metadata type also failed, using raw string",
						zap.String("node_id", cmd.NodeID),
						zap.String("raw_value", cmd.Value),
						zap.String("metadata_type", metadataType),
						zap.Error(err))
					writeValue = cmd.Value
				} else {
					convertedType = metadataType
				}
			} else {
				// 元数据类型为空或与用户类型相同，无回退目标，兜底原始字符串
				writeValue = cmd.Value
			}
		} else {
			// 用户类型转换成功
			convertedType = userType
		}
	} else if metadataType != "" && metadataType != "String" {
		// 用户未指定类型，以元数据类型为目标进行转换
		writeValue, err = opcua.ConvertWriteValue(cmd.Value, metadataType)
		if err != nil {
			// 元数据类型转换失败，兜底原始字符串
			h.logger.Warn("Value conversion with metadata type failed, using raw string",
				zap.String("node_id", cmd.NodeID),
				zap.String("raw_value", cmd.Value),
				zap.String("metadata_type", metadataType),
				zap.Error(err))
			writeValue = cmd.Value
		} else {
			convertedType = metadataType
		}
	} else {
		// 无用户类型也无元数据类型，原样传递字符串
		writeValue = cmd.Value
	}

	h.logger.Info("Write value prepared",
		zap.String("node_id", cmd.NodeID),
		zap.String("user_type", userType),
		zap.String("metadata_type", metadataType),
		zap.String("converted_type", convertedType),
		zap.Any("value", writeValue),
		zap.String("value_kind", fmt.Sprintf("%T", writeValue)))

	h.logger.Debug("Write value",
		zap.String("node_id", cmd.NodeID),
		zap.Any("value", writeValue),
		zap.String("value_kind", fmt.Sprintf("%T", writeValue)))

	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	writeErr := h.opcuaClient.Write(writeCtx, cmd.NodeID, writeValue)
	if writeErr != nil {
		// 首次写入失败时，若用户类型与元数据类型不兼容（跨族），
		// 尝试用元数据类型重试一次。例如用户用 boolean 写入 Int32 节点，
		// 首次因类型不匹配被 KepServer 拒绝，此处用 Int32 重试。
		if userType != "" && metadataType != "" && !isCompatibleTypeGroup(userType, metadataType) {
			h.logger.Warn("Write failed with user type, retrying with metadata type",
				zap.String("node_id", cmd.NodeID),
				zap.String("user_type", userType),
				zap.String("metadata_type", metadataType),
				zap.Error(writeErr))
			retryValue, retryErr := opcua.ConvertWriteValue(cmd.Value, metadataType)
			if retryErr == nil {
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer retryCancel()
				var retryWriteErr error
				retryWriteErr = h.opcuaClient.Write(retryCtx, cmd.NodeID, retryValue)
				if retryWriteErr == nil {
					h.logger.Info("Write succeeded with metadata type retry",
						zap.String("node_id", cmd.NodeID),
						zap.String("metadata_type", metadataType),
						zap.Any("value", retryValue))
					h.publishResult(model.WriteResult{
						NodeID:       cmd.NodeID,
						RequestID:    cmd.RequestID,
						Success:      true,
						WrittenValue: retryValue,
					})
					return
				}
				h.logger.Error("Write retry with metadata type also failed",
					zap.String("node_id", cmd.NodeID),
					zap.Error(retryWriteErr))
			} else {
				h.logger.Warn("Metadata type conversion failed in retry",
					zap.String("node_id", cmd.NodeID),
					zap.Error(retryErr))
			}
		}
		h.logger.Error("Write failed",
			zap.String("node_id", cmd.NodeID),
			zap.Any("value", writeValue),
			zap.Error(writeErr))
		h.publishResult(model.WriteResult{
			NodeID:    cmd.NodeID,
			RequestID: cmd.RequestID,
			Error:     writeErr.Error(),
		})
		return
	}

	h.logger.Info("Write succeeded",
		zap.String("node_id", cmd.NodeID),
		zap.Any("value", writeValue))

	h.publishResult(model.WriteResult{
		NodeID:       cmd.NodeID,
		RequestID:    cmd.RequestID,
		Success:      true,
		WrittenValue: writeValue,
	})
}

// typeGroup 将类型字符串归类为数值族，用于跨族兼容性判断。
// 返回值：1=bool, 2=integer, 3=float, 4=string, 0=unknown。
// 同族类型写入同一节点通常可被 KepServer 接受。
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

// isCompatibleTypeGroup 判断用户类型与元数据类型是否属于同一类型族。
// 元数据未知或为 String 时视为兼容（不做限制）；
// 任一类型无法识别时视为兼容（不做限制，仅记录告警）；
// 同族类型（如 int32→int64）视为兼容。
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
