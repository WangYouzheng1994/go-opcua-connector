// Package opcua provides OPC UA client management including connection, subscription, and data reading.
package opcua

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

// Client OPC UA客户端管理器，负责与OPC UA服务器的连接、订阅管理
type Client struct {
	// config OPC UA配置
	config *config.OPCUAConfig
	// client gopcua底层客户端
	client *opcua.Client
	// logger 日志记录器
	logger *zap.Logger
	// mu 读写锁，保护连接状态和客户端实例
	mu sync.RWMutex
}

// DataChangeHandler 回调函数类型，用于处理数据变化
type DataChangeHandler func(points []model.DataPoint)

// qualityString 将StatusCode映射为简洁的品质字符串
// OPC UA规范：Good=0x00, Uncertain=0x40, Bad=0x80
func qualityString(status ua.StatusCode) string {
	switch status & 0xC0000000 {
	case 0x00000000:
		return "Good"
	case 0x40000000:
		return "Uncertain"
	default:
		return "Bad"
	}
}

// NewClient 创建新的OPC UA客户端
func NewClient(cfg *config.OPCUAConfig, logger *zap.Logger) *Client {
	return &Client{
		config: cfg,
		logger: logger,
	}
}

// Connect 连接到OPC UA服务器
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	endpoint := c.config.Endpoint

	opts := []opcua.Option{
		opcua.SecurityPolicy(c.config.SecurityPolicy),
		opcua.SecurityMode(c.parseSecurityMode()),
	}

	if c.config.Username != "" && c.config.Password != "" {
		opts = append(opts, opcua.AuthUsername(c.config.Username, c.config.Password))
	}

	// 初始化opcua的客户端
	client, err := opcua.NewClient(endpoint, opts...)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	c.client = client

	c.logger.Info("Connecting to OPC UA server (TCP + Session)...")
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.logger.Info("Connected to OPC UA server", zap.String("endpoint", endpoint))
	return nil
}

// Subscribe 创建订阅并注册监控项
func (c *Client) Subscribe(ctx context.Context, nodes []string, topic string, handler DataChangeHandler) error {
	if c.client == nil {
		return fmt.Errorf("client not connected, call Connect() first")
	}

	notificationCh := make(chan *opcua.PublishNotificationData, 100)

	sub, err := c.client.Subscribe(ctx, &opcua.SubscriptionParameters{
		Interval: 100,
	}, notificationCh)
	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}

	monitorItems := make([]*ua.MonitoredItemCreateRequest, len(nodes))
	for i, node := range nodes {
		nodeID, err := ua.ParseNodeID(node)
		if err != nil {
			return fmt.Errorf("invalid node ID %s: %w", node, err)
		}
		monitorItems[i] = opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID, ua.AttributeIDValue, uint32(i))
	}

	go c.handleNotifications(sub, notificationCh, topic, handler, nodes)

	_, err = sub.Monitor(ctx, ua.TimestampsToReturnBoth, monitorItems...)
	if err != nil {
		return fmt.Errorf("failed to create monitored items: %w", err)
	}

	c.logger.Info("Subscription created",
		zap.Int("node_count", len(nodes)),
		zap.Uint32("subscription_id", sub.SubscriptionID))

	return nil
}

// handleNotifications 处理OPC UA订阅通知数据
// 解析DataChangeNotification，通过ClientHandle映射回NodeID
func (c *Client) handleNotifications(
	sub *opcua.Subscription,
	notificationCh chan *opcua.PublishNotificationData,
	topic string,
	handler DataChangeHandler,
	nodes []string,
) {
	for {
		select {
		case notification, ok := <-notificationCh:
			if !ok {
				return
			}

			if notification.Error != nil {
				c.logger.Error("Publish error", zap.Error(notification.Error))
				continue
			}

			if notification.Value == nil {
				continue
			}

			data, ok := notification.Value.(*ua.DataChangeNotification)
			if !ok {
				continue
			}

			points := make([]model.DataPoint, 0, len(data.MonitoredItems))
			for _, item := range data.MonitoredItems {
				nodeID := "unknown"
				if int(item.ClientHandle) < len(nodes) {
					nodeID = nodes[item.ClientHandle]
				}

				dp := model.DataPoint{
					NodeID: nodeID,
					Topic:  topic,
				}

				if item.Value != nil {
					if item.Value.Value != nil {
						dp.Value = item.Value.Value.Value()
					}
					dp.Quality = qualityString(item.Value.Status)
					if !item.Value.ServerTimestamp.IsZero() {
						dp.Timestamp = item.Value.ServerTimestamp
					}
				} else {
					dp.Quality = "BadNoValue"
					dp.Timestamp = time.Now()
				}

				points = append(points, dp)
			}

			if len(points) > 0 {
				handler(points)
			}
		}
	}
}

// Close 关闭客户端连接
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		c.client.Close(context.Background())
		c.client = nil
	}

	c.logger.Info("OPC UA client closed")
}

// parseSecurityMode 解析安全模式配置
func (c *Client) parseSecurityMode() ua.MessageSecurityMode {
	switch c.config.SecurityMode {
	case "Sign":
		return ua.MessageSecurityModeSign
	case "SignAndEncrypt":
		return ua.MessageSecurityModeSignAndEncrypt
	default:
		return ua.MessageSecurityModeNone
	}
}

// IsConnected 检查连接状态
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client != nil
}

// ResolveNodes 解析配置节点列表，自动展开文件夹节点为叶子变量节点
// 对每个配置节点尝试Browse子节点：
// - 无Variable子节点 → 视为叶子节点，直接保留
// - 有Variable子节点（精确模式） → 展开为直接子节点列表
// - 有Variable子节点（通配模式.*） → 穿透Object子文件夹，递归收集所有叶子
func (c *Client) ResolveNodes(ctx context.Context, nodeIDs []string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	resolved := make([]string, 0, len(nodeIDs))

	for _, rawNodeID := range nodeIDs {
		recursive := false
		cleanNodeID := rawNodeID

		// 检测.*通配符后缀，决定是否递归穿透Object文件夹
		if strings.HasSuffix(rawNodeID, ".*") {
			recursive = true
			cleanNodeID = rawNodeID[:len(rawNodeID)-2]
		}

		id, err := ua.ParseNodeID(cleanNodeID)
		if err != nil {
			c.logger.Warn("Invalid node ID, skipping", zap.String("node_id", rawNodeID), zap.Error(err))
			continue
		}

		var leaves []string
		if recursive {
			leaves, err = c.browseAllLeaves(ctx, id)
		} else {
			leaves, err = c.browseVariableLeaves(ctx, id)
		}

		if err != nil {
			c.logger.Warn("Failed to browse node, using as-is",
				zap.String("node_id", rawNodeID), zap.Error(err))
			resolved = append(resolved, cleanNodeID)
			continue
		}

		if len(leaves) == 0 {
			resolved = append(resolved, cleanNodeID)
		} else {
			c.logger.Info("Node expanded to leaf variables",
				zap.String("folder_node", rawNodeID),
				zap.Bool("recursive", recursive),
				zap.Int("leaf_count", len(leaves)))
			resolved = append(resolved, leaves...)
		}
	}

	return resolved, nil
}

// browseVariableLeaves 浏览节点的直接Variable子节点，仅取当前层级
func (c *Client) browseVariableLeaves(ctx context.Context, nodeID *ua.NodeID) ([]string, error) {
	node := c.client.Node(nodeID)

	refs, err := node.References(ctx, 0, ua.BrowseDirectionForward, ua.NodeClassVariable, true)
	if err != nil {
		return nil, err
	}

	leaves := make([]string, 0, len(refs))
	for _, ref := range refs {
		childNode := c.client.NodeFromExpandedNodeID(ref.NodeID)
		if childNode == nil || childNode.ID == nil {
			continue
		}
		leaves = append(leaves, childNode.ID.String())
	}

	return leaves, nil
}

// browseAllLeaves 递归浏览节点及其所有Object子文件夹，收集所有叶子Variable类型节点的NodeID
// 跳过_开头的KepServer元数据节点
func (c *Client) browseAllLeaves(ctx context.Context, nodeID *ua.NodeID) ([]string, error) {
	node := c.client.Node(nodeID)

	// 收集当前层级的Variable子节点
	varRefs, err := node.References(ctx, 0, ua.BrowseDirectionForward, ua.NodeClassVariable, true)
	if err != nil {
		return nil, err
	}

	leaves := make([]string, 0)
	for _, ref := range varRefs {
		childNode := c.client.NodeFromExpandedNodeID(ref.NodeID)
		if childNode == nil || childNode.ID == nil {
			continue
		}
		leaves = append(leaves, childNode.ID.String())
	}

	// 递归进入Object子文件夹，跳过_开头的KepServer元数据节点
	objRefs, err := node.References(ctx, 0, ua.BrowseDirectionForward, ua.NodeClassObject, true)
	if err != nil {
		return leaves, nil
	}

	for _, ref := range objRefs {
		if strings.HasPrefix(ref.BrowseName.Name, "_") {
			continue
		}
		childNode := c.client.NodeFromExpandedNodeID(ref.NodeID)
		if childNode == nil || childNode.ID == nil {
			continue
		}
		childLeaves, err := c.browseAllLeaves(ctx, childNode.ID)
		if err != nil {
			c.logger.Warn("Failed to browse child folder, skipping",
				zap.String("node_id", childNode.ID.String()),
				zap.Error(err))
			continue
		}
		leaves = append(leaves, childLeaves...)
	}

	return leaves, nil
}

// Read 读取单个节点的值（主动拉取）
func (c *Client) Read(ctx context.Context, nodeID string) (*model.DataPoint, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	id, err := ua.ParseNodeID(nodeID)
	if err != nil {
		return nil, fmt.Errorf("invalid node ID: %w", err)
	}

	req := &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{
				NodeID:       id,
				AttributeID:  ua.AttributeIDValue,
				IndexRange:   "",
				DataEncoding: nil,
			},
		},
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := c.client.Read(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	if resp.Results == nil || len(resp.Results) == 0 {
		return nil, fmt.Errorf("no results returned")
	}

	result := resp.Results[0]
	point := &model.DataPoint{
		NodeID:    nodeID,
		Quality:   qualityString(result.Status),
		Timestamp: result.ServerTimestamp,
	}
	if result.Value != nil {
		point.Value = result.Value.Value()
	}

	return point, nil
}

// ReadAll 批量读取多个节点的值（主动拉取，用于心跳验证）
func (c *Client) ReadAll(ctx context.Context, nodeIDs []string) ([]model.DataPoint, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	nodesToRead := make([]*ua.ReadValueID, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		id, err := ua.ParseNodeID(nodeID)
		if err != nil {
			c.logger.Warn("Invalid node ID, skipping", zap.String("node_id", nodeID), zap.Error(err))
			continue
		}
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID:       id,
			AttributeID:  ua.AttributeIDValue,
			IndexRange:   "",
			DataEncoding: nil,
		})
	}

	if len(nodesToRead) == 0 {
		return nil, nil
	}

	req := &ua.ReadRequest{
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := c.client.Read(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	points := make([]model.DataPoint, 0, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		if i >= len(resp.Results) {
			break
		}

		result := resp.Results[i]
		point := model.DataPoint{
			NodeID:    nodeID,
			Quality:   qualityString(result.Status),
			Timestamp: result.ServerTimestamp,
		}
		if result.Value != nil {
			point.Value = result.Value.Value()
		}
		points = append(points, point)
	}

	return points, nil
}

// Write 向OPC UA服务器单个节点写入值
func (c *Client) Write(ctx context.Context, nodeID string, value any) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client == nil {
		return fmt.Errorf("client not connected")
	}

	id, err := ua.ParseNodeID(nodeID)
	if err != nil {
		return fmt.Errorf("invalid node ID: %w", err)
	}

	variant, err := ua.NewVariant(value)
	if err != nil {
		return fmt.Errorf("failed to create variant: %w", err)
	}

	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			{
				NodeID:      id,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					Value: variant,
				},
			},
		},
	}

	resp, err := c.client.Write(ctx, req)
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	if len(resp.Results) > 0 && resp.Results[0] != ua.StatusOK {
		return fmt.Errorf("write rejected: %s", resp.Results[0])
	}

	return nil
}

// WriteAll 批量向OPC UA服务器写入值，每项含NodeID和对应值
func (c *Client) WriteAll(ctx context.Context, items map[string]any) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client == nil {
		return fmt.Errorf("client not connected")
	}

	nodesToWrite := make([]*ua.WriteValue, 0, len(items))
	for nodeID, value := range items {
		id, err := ua.ParseNodeID(nodeID)
		if err != nil {
			return fmt.Errorf("invalid node ID %s: %w", nodeID, err)
		}

		variant, err := ua.NewVariant(value)
		if err != nil {
			return fmt.Errorf("failed to create variant for %s: %w", nodeID, err)
		}

		nodesToWrite = append(nodesToWrite, &ua.WriteValue{
			NodeID:      id,
			AttributeID: ua.AttributeIDValue,
			Value: &ua.DataValue{
				Value: variant,
			},
		})
	}

	req := &ua.WriteRequest{
		NodesToWrite: nodesToWrite,
	}

	resp, err := c.client.Write(ctx, req)
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	for _, result := range resp.Results {
		if result != ua.StatusOK {
			return fmt.Errorf("write rejected for one or more nodes: %s", result)
		}
	}

	return nil
}

// ConvertWriteValue 将字符串值按目标类型转换为OPC UA支持的Go类型
// valueType 可选：int32 / int64 / float32 / float64 / bool / string
// valueType 为空时自动推断：bool → float64 → string
func ConvertWriteValue(raw string, valueType string) (any, error) {
	switch strings.ToLower(valueType) {
	case "bool":
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to bool: %w", raw, err)
		}
		return b, nil
	case "int32":
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to int32: %w", raw, err)
		}
		return int32(v), nil
	case "int64":
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to int64: %w", raw, err)
		}
		return v, nil
	case "float32":
		v, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to float32: %w", raw, err)
		}
		return float32(v), nil
	case "float64":
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to float64: %w", raw, err)
		}
		return v, nil
	case "string":
		return raw, nil
	default:
		return autoConvert(raw)
	}
}

// autoConvert 自动推断字符串值的类型：bool → float64 → string
func autoConvert(raw string) (any, error) {
	if raw == "" {
		return raw, nil
	}

	if b, err := strconv.ParseBool(raw); err == nil {
		return b, nil
	}

	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}

	return raw, nil
}
