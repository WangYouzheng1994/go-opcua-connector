package opcua

import (
	"context"
	"fmt"
	"sync"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

/**
 * OPC UA客户端管理器
 * 负责与OPC UA服务器的连接、订阅管理
 *
 * @author 王有政
 */
type Client struct {
	config *config.OPCUAConfig
	client *opcua.Client
	logger *zap.Logger
	mu     sync.RWMutex
}

/**
 * 回调函数类型，用于处理数据变化
 *
 * @author 王有政
 */
type DataChangeHandler func(points []model.DataPoint)

/**
 * 创建新的OPC UA客户端
 *
 * @author 王有政
 */
func NewClient(cfg *config.OPCUAConfig, logger *zap.Logger) *Client {
	return &Client{
		config: cfg,
		logger: logger,
	}
}

/**
 * 连接到OPC UA服务器
 *
 * @author 王有政
 */
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

/**
 * 创建订阅并注册监控项
 *
 * @author 王有政
 */
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

	go c.handleNotifications(sub, notificationCh, topic, handler)

	_, err = sub.Monitor(ctx, ua.TimestampsToReturnBoth, monitorItems...)
	if err != nil {
		return fmt.Errorf("failed to create monitored items: %w", err)
	}

	c.logger.Info("Subscription created",
		zap.Int("node_count", len(nodes)),
		zap.Uint32("subscription_id", sub.SubscriptionID))

	return nil
}

/**
 * 处理通知数据
 *
 * @author 王有政
 */
func (c *Client) handleNotifications(
	sub *opcua.Subscription,
	notificationCh chan *opcua.PublishNotificationData,
	topic string,
	handler DataChangeHandler,
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

			point := model.DataPoint{
				NodeID:    "unknown",
				Value:     notification,
				Quality:   "Good",
				Topic:     topic,
			}

			handler([]model.DataPoint{point})
		}
	}
}

/**
 * 关闭客户端连接
 *
 * @author 王有政
 */
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		c.client.Close(context.Background())
		c.client = nil
	}

	c.logger.Info("OPC UA client closed")
}

/**
 * 解析安全模式配置
 *
 * @author 王有政
 */
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

/**
 * 检查连接状态
 *
 * @author 王有政
 */
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client != nil
}

/**
 * 读取单个节点的值（主动拉取）
 *
 * @author 王有政
 */
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
				NodeID:          id,
				AttributeID:     ua.AttributeIDValue,
				IndexRange:      "",
				DataEncoding:    nil,
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
		Value:     result.Value,
		Quality:   fmt.Sprintf("%v", result.Status),
		Timestamp: result.ServerTimestamp,
	}

	return point, nil
}

/**
 * 批量读取多个节点的值（主动拉取，用于心跳验证）
 *
 * @author 王有政
 */
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
		NodesToRead:         nodesToRead,
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
			Value:     result.Value,
			Quality:   fmt.Sprintf("%v", result.Status),
			Timestamp: result.ServerTimestamp,
		}
		points = append(points, point)
	}

	return points, nil
}
