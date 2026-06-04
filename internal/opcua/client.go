// Package opcua provides OPC UA client management including connection, subscription, and data reading.
package opcua

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/model"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
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

// Connect 连接到OPC UA服务器，包含端点发现、安全协商、会话建立。1
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	endpoint := c.config.Endpoint

	// 第一步：加载客户端证书和私钥（支持 PEM/DER 证书，PKCS#1/PKCS#8 私钥）。
	var certOpt, keyOpt opcua.Option
	if c.config.Certificate != "" && c.config.PrivateKey != "" {
		cert, err := loadCertFile(c.config.Certificate)
		if err != nil {
			return fmt.Errorf("failed to load certificate: %w", err)
		}
		key, err := loadPrivateKeyFile(c.config.PrivateKey)
		if err != nil {
			return fmt.Errorf("failed to load private key: %w", err)
		}
		certOpt = opcua.Certificate(cert)
		keyOpt = opcua.PrivateKey(key)
		c.logger.Info("Loading client certificate",
			zap.String("certificate", c.config.Certificate),
			zap.String("private_key", c.config.PrivateKey))
	}

	// 第二步：端点发现，获取服务器端点列表（含服务器证书）。
	getEndpointsOpts := []opcua.Option{
		opcua.RequestTimeout(time.Duration(c.config.RequestTimeout) * time.Second),
	}
	if certOpt != nil {
		getEndpointsOpts = append(getEndpointsOpts, certOpt)
	}

	c.logger.Info("Discovering OPC UA endpoints...", zap.String("endpoint", endpoint))
	endpoints, err := opcua.GetEndpoints(ctx, endpoint, getEndpointsOpts...)
	if err != nil {
		return fmt.Errorf("failed to get endpoints: %w", err)
	}

	// 第三步：选择匹配的端点（按安全策略+安全模式筛选）。
	selectedEp := opcua.SelectEndpoint(endpoints, c.config.SecurityPolicy, c.parseSecurityMode())
	if selectedEp == nil {
		c.logger.Warn("No matching endpoint found, falling back to None security",
			zap.String("policy", c.config.SecurityPolicy),
			zap.String("mode", c.config.SecurityMode))
		selectedEp = opcua.SelectEndpoint(endpoints, ua.SecurityPolicyURINone, ua.MessageSecurityModeNone)
	}
	if selectedEp == nil {
		return fmt.Errorf("no suitable endpoint found for policy=%s mode=%s", c.config.SecurityPolicy, c.config.SecurityMode)
	}
	c.logger.Info("Selected OPC UA endpoint",
		zap.String("security_policy", selectedEp.SecurityPolicyURI),
		zap.Any("security_mode", selectedEp.SecurityMode))

	// 第四步：使用 SecurityFromEndpoint 构建客户端选项（自动配置服务器证书）。
	opts := []opcua.Option{
		opcua.SecurityFromEndpoint(selectedEp, ua.UserTokenTypeAnonymous),
		opcua.RequestTimeout(time.Duration(c.config.RequestTimeout) * time.Second),
		opcua.SessionTimeout(1 * time.Hour),
		opcua.Lifetime(2 * time.Hour),
	}
	if certOpt != nil {
		opts = append(opts, certOpt, keyOpt)
	}
	if c.config.Username != "" && c.config.Password != "" {
		opts = append(opts, opcua.AuthUsername(c.config.Username, c.config.Password))
	}

	// 第五步：创建客户端并连接。
	client, err := opcua.NewClient(endpoint, opts...)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	c.client = client

	c.logger.Info("Connecting to OPC UA server (TCP + Session)...")
	if err := client.Connect(ctx); err != nil {
		c.client = nil
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.logger.Info("Connected to OPC UA server", zap.String("endpoint", endpoint))
	return nil
}

// Subscribe 创建订阅并注册监控项。
// 节点数>5000时自动拆分为多个Subscription分摊KepServer内部负载。
func (c *Client) Subscribe(ctx context.Context, nodes []string, topic string, handler DataChangeHandler) error {
	c.mu.RLock()
	cli := c.client
	c.mu.RUnlock()

	if cli == nil {
		return fmt.Errorf("client not connected, call Connect() first")
	}

	// 预先构建所有MonitoredItem，ClientHandle 为全局节点索引
	monitorItems := make([]*ua.MonitoredItemCreateRequest, len(nodes))
	for i, node := range nodes {
		nodeID, err := ua.ParseNodeID(node)
		if err != nil {
			return fmt.Errorf("invalid node ID %s: %w", node, err)
		}
		monitorItems[i] = opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID, ua.AttributeIDValue, uint32(i))
	}

	// 每个 Subscription 最多挂载 2000 个 MonitoredItem，
	// 一次 Monitor 搞定，不内层分批。
	const itemsPerSub = 2000
	subCount := (len(monitorItems) + itemsPerSub - 1) / itemsPerSub
	batchTimeout := time.Duration(c.config.RequestTimeout) * time.Second

	// mergedCh 汇总所有 Subscription 的通知，统一由 handleNotifications 消费
	mergedCh := make(chan *opcua.PublishNotificationData, 1000)

	c.logger.Info("Registering monitored items with multiple subscriptions",
		zap.Int("total_items", len(monitorItems)),
		zap.Int("items_per_sub", itemsPerSub),
		zap.Int("sub_count", subCount),
		zap.Duration("timeout_per_batch", batchTimeout))

	for subIdx := 0; subIdx < subCount; subIdx++ {
		subStart := subIdx * itemsPerSub
		subEnd := subStart + itemsPerSub
		if subEnd > len(monitorItems) {
			subEnd = len(monitorItems)
		}
		itemsInSub := monitorItems[subStart:subEnd]

		subCh := make(chan *opcua.PublishNotificationData, 100)
		sub, err := cli.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: 100,
		}, subCh)
		if err != nil {
			return fmt.Errorf("failed to create subscription %d/%d: %w", subIdx+1, subCount, err)
		}

		// 将该 Subscription 的通知转到汇总通道
		go func(ch chan *opcua.PublishNotificationData) {
			for n := range ch {
				mergedCh <- n
			}
		}(subCh)

		c.logger.Info("Creating monitored items for subscription",
			zap.Int("sub", subIdx+1),
			zap.Int("sub_count", subCount),
			zap.Int("items", len(itemsInSub)))

		batchCtx, batchCancel := context.WithTimeout(ctx, batchTimeout)
		start := time.Now()
		_, err = sub.Monitor(batchCtx, ua.TimestampsToReturnBoth, itemsInSub...)
		elapsed := time.Since(start)
		batchCancel()

		if err != nil {
			c.logger.Error("Failed to create monitored items for subscription",
				zap.Int("sub", subIdx+1),
				zap.Int("sub_count", subCount),
				zap.Int("items", len(itemsInSub)),
				zap.Duration("elapsed", elapsed),
				zap.Error(err))
			return fmt.Errorf("failed to create monitored items (sub %d/%d): %w", subIdx+1, subCount, err)
		}

		c.logger.Info("Monitored items created",
			zap.Int("sub", subIdx+1),
			zap.Int("sub_count", subCount),
			zap.Int("items", len(itemsInSub)),
			zap.Duration("elapsed", elapsed))

		c.logger.Info("Subscription registered",
			zap.Int("sub", subIdx+1),
			zap.Int("sub_count", subCount),
			zap.Int("items", len(itemsInSub)),
			zap.Uint32("subscription_id", sub.SubscriptionID))
	}

	go c.handleNotifications(mergedCh, topic, handler, nodes)

	c.logger.Info("All subscriptions created",
		zap.Int("node_count", len(nodes)),
		zap.Int("sub_count", subCount))

	return nil
}

// handleNotifications 处理OPC UA订阅通知数据
// 解析DataChangeNotification，通过ClientHandle映射回NodeID
func (c *Client) handleNotifications(
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

// Close 关闭客户端连接，不在持锁期间执行网络I/O。
func (c *Client) Close() {
	c.mu.Lock()
	cli := c.client
	c.client = nil
	c.mu.Unlock()

	if cli != nil {
		cli.Close(context.Background())
	}

	c.logger.Info("OPC UA client closed")
}

// Reconnect 关闭旧连接后重新连接 OPC UA 服务器
// 用于长时间运行中会话过期后的恢复
func (c *Client) Reconnect(ctx context.Context) error {
	c.mu.Lock()
	if c.client != nil {
		c.client.Close(context.Background())
		c.client = nil
	}
	c.mu.Unlock()

	c.logger.Info("Reconnecting to OPC UA server")
	return c.Connect(ctx)
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

// loadCertFile 加载证书文件，自动检测 PEM 或 DER 格式。
func loadCertFile(filename string) ([]byte, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}
	// 不看扩展名，直接检测内容是否为 PEM 格式。
	block, _ := pem.Decode(b)
	if block != nil && block.Type == "CERTIFICATE" {
		return block.Bytes, nil
	}
	return b, nil
}

// loadPrivateKeyFile 加载私钥文件，自动检测 PEM/DER 格式，兼容 PKCS#1 和 PKCS#8。
func loadPrivateKeyFile(filename string) (*rsa.PrivateKey, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}

	derBytes := b
	// 不看扩展名，直接检测内容是否为 PEM 格式。
	block, _ := pem.Decode(b)
	if block != nil {
		derBytes = block.Bytes
	}

	// 优先尝试 PKCS#1 格式。
	if key, err := x509.ParsePKCS1PrivateKey(derBytes); err == nil {
		return key, nil
	}

	// 回退到 PKCS#8 格式。
	keyAny, err := x509.ParsePKCS8PrivateKey(derBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key (tried PKCS#1 and PKCS#8): %w", err)
	}
	key, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not RSA (type: %T)", keyAny)
	}
	return key, nil
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

// browseVariableLeaves 浏览节点的直接Variable子节点，仅取当前层级，仅 HasComponent 引用类型（排除 HasProperty 的元数据属性节点）
func (c *Client) browseVariableLeaves(ctx context.Context, nodeID *ua.NodeID) ([]string, error) {
	node := c.client.Node(nodeID)

	refs, err := node.References(ctx, id.HasComponent, ua.BrowseDirectionForward, ua.NodeClassVariable, true)
	if err != nil {
		return nil, err
	}

	leaves := make([]string, 0, len(refs))
	for _, ref := range refs {
		if strings.HasPrefix(ref.BrowseName.Name, "_") {
			continue
		}
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

	// 收集当前层级的Variable子节点，仅 HasComponent 引用类型（排除 HasProperty 的元数据属性节点）
	varRefs, err := node.References(ctx, id.HasComponent, ua.BrowseDirectionForward, ua.NodeClassVariable, true)
	if err != nil {
		return nil, err
	}

	leaves := make([]string, 0)
	for _, ref := range varRefs {
		// 额外兜底：过滤 _ 开头的 BrowseName（部分非标准 Server 可能不用 HasProperty）
		if strings.HasPrefix(ref.BrowseName.Name, "_") {
			continue
		}
		childNode := c.client.NodeFromExpandedNodeID(ref.NodeID)
		if childNode == nil || childNode.ID == nil {
			continue
		}
		leaves = append(leaves, childNode.ID.String())
	}

	// 递归进入Object子文件夹，跳过_开头的KepServer元数据节点
	objRefs, err := node.References(ctx, id.Organizes, ua.BrowseDirectionForward, ua.NodeClassObject, true)
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
	cli := c.client
	c.mu.RUnlock()

	if cli == nil {
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

	resp, err := cli.Read(ctx, req)
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
	cli := c.client
	c.mu.RUnlock()

	if cli == nil {
		return nil, fmt.Errorf("client not connected")
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	nodesToRead := make([]*ua.ReadValueID, 0, len(nodeIDs))
	validNodeIDs := make([]string, 0, len(nodeIDs))
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
		validNodeIDs = append(validNodeIDs, nodeID)
	}

	if len(nodesToRead) == 0 {
		return nil, nil
	}

	req := &ua.ReadRequest{
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := cli.Read(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	points := make([]model.DataPoint, 0, len(validNodeIDs))
	for i, nodeID := range validNodeIDs {
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
	cli := c.client
	c.mu.RUnlock()

	if cli == nil {
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

	c.logger.Info("Write variant created",
		zap.String("node_id", nodeID),
		zap.Any("value", value),
		zap.Any("variant_type", variant.Type()),
		zap.Any("variant_type_int", int(variant.Type())))

	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			{
				NodeID:      id,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					EncodingMask: ua.DataValueValue,
					Value:        variant,
				},
			},
		},
	}

	resp, err := cli.Write(ctx, req)
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
	cli := c.client
	c.mu.RUnlock()

	if cli == nil {
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

	resp, err := cli.Write(ctx, req)
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

// ReadNodeDataTypes 批量读取节点的 DataType 属性，用于回写时自动类型适配
func (c *Client) ReadNodeDataTypes(ctx context.Context, nodeIDs []string) (map[string]string, error) {
	c.mu.RLock()
	cli := c.client
	c.mu.RUnlock()

	if cli == nil {
		return nil, fmt.Errorf("client not connected")
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	nodesToRead := make([]*ua.ReadValueID, 0, len(nodeIDs))
	nodeIDList := make([]string, 0, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		id, err := ua.ParseNodeID(nodeID)
		if err != nil {
			c.logger.Warn("Invalid node ID, skipping", zap.String("node_id", nodeID), zap.Error(err))
			continue
		}
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID:       id,
			AttributeID:  ua.AttributeIDDataType,
			IndexRange:   "",
			DataEncoding: nil,
		})
		nodeIDList = append(nodeIDList, nodeID)
	}

	if len(nodesToRead) == 0 {
		return map[string]string{}, nil
	}

	req := &ua.ReadRequest{
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnNeither,
	}

	resp, err := cli.Read(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("read data types failed: %w", err)
	}

	result := make(map[string]string, len(nodeIDList))
	for i, nodeResult := range resp.Results {
		if i >= len(nodeIDList) {
			break
		}
		nodeID := nodeIDList[i]
		if nodeResult.Status != ua.StatusOK {
			c.logger.Warn("Failed to read data type for node",
				zap.String("node_id", nodeID),
				zap.String("status", fmt.Sprintf("%s", nodeResult.Status)))
			// 仅当结果中不存在时才设置默认值，防止覆盖有效数据
			if _, exists := result[nodeID]; !exists {
				result[nodeID] = "String"
			}
			continue
		}

		if nodeResult.Value != nil && nodeResult.Value.NodeID() != nil {
			result[nodeID] = typeIDToString(nodeResult.Value.NodeID())
		} else {
			result[nodeID] = "String"
		}
	}

	return result, nil
}

// typeIDToString 将 OPC UA TypeID 转换为可读字符串
func typeIDToString(typeID *ua.NodeID) string {
	if typeID == nil {
		return "String"
	}

	switch typeID.IntID() {
	case 0:
		return "Null"
	case 1:
		return "Boolean"
	case 2:
		return "SByte"
	case 3:
		return "Byte"
	case 4:
		return "Int16"
	case 5:
		return "UInt16"
	case 6:
		return "Int32"
	case 7:
		return "UInt32"
	case 8:
		return "Int64"
	case 9:
		return "UInt64"
	case 10:
		return "Float"
	case 11:
		return "Double"
	case 12:
		return "String"
	case 13:
		return "DateTime"
	case 14:
		return "GUID"
	case 15:
		return "ByteString"
	case 16:
		return "XML"
	case 17:
		return "NodeID"
	case 18:
		return "StatusCode"
	case 19:
		return "QualifiedName"
	case 20:
		return "LocalizedText"
	case 21:
		return "ExtensionObject"
	case 22:
		return "DataValue"
	case 23:
		return "Variant"
	case 24:
		return "DiagnosticInfo"
	default:
		return fmt.Sprintf("NodeID:%s", typeID.String())
	}
}

// coerceToInt 将字符串强制转换为有符号整数。
// 三级回退策略：
//  1. 数值解析：直接 ParseInt
//  2. bool 映射： "true"/"1"→1, "false"/"0"→0
//  3. 浮点截断：ParseFloat 后转换为整数，越界时报错
func coerceToInt(raw string, bitSize int) (int64, error) {
	v, err := strconv.ParseInt(raw, 10, bitSize)
	if err == nil {
		return v, nil
	}
	// 一级回退：解析为布尔值后映射到 1/0
	if b, ok := parseBoolish(raw); ok {
		if b {
			return 1, nil
		}
		return 0, nil
	}
	// 二级回退：解析为浮点数后截断为整数
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		if f > math.MaxInt64 || f < math.MinInt64 {
			return 0, fmt.Errorf("float %v out of int64 range", f)
		}
		return int64(f), nil
	}
	return 0, fmt.Errorf("cannot coerce %q to int", raw)
}

// coerceToUint 将字符串强制转换为无符号整数。
// 三级回退策略同 coerceToInt，但浮点数负值会被拒绝。
func coerceToUint(raw string, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(raw, 10, bitSize)
	if err == nil {
		return v, nil
	}
	// 一级回退：布尔值映射到 1/0
	if b, ok := parseBoolish(raw); ok {
		if b {
			return 1, nil
		}
		return 0, nil
	}
	// 二级回退：浮点数截断，负值和非数字会被拒绝
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		if f < 0 || f > math.MaxUint64 {
			return 0, fmt.Errorf("float %v out of uint64 range", f)
		}
		return uint64(f), nil
	}
	return 0, fmt.Errorf("cannot coerce %q to uint", raw)
}

// parseBoolish 尝试解析类布尔值，strconv.ParseBool 已覆盖 "1"/"0"/"true"/"false"/"t"/"f" 等。
// 返回值和是否成功。
func parseBoolish(raw string) (bool, bool) {
	b, err := strconv.ParseBool(raw)
	return b, err == nil
}

// ConvertWriteValue 将字符串值按目标类型转换为OPC UA支持的Go类型。
// 三层强转策略，按优先级依次尝试：
//   - 数值目标：数值解析 → bool(1/0) → 浮点截断
//   - 布尔目标：ParseBool → "1"/"0" → 浮点非零判断
//   - 浮点目标：ParseFloat → bool(1.0/0.0)
//
// valueType 可选：bool / boolean / sbyte / byte / int16 / uint16 / int32 / uint32 / int64 / uint64 / float / float32 / double / float64 / string
// valueType 为空或未知时走 autoConvert 自动推断：bool → float64 → string
func ConvertWriteValue(raw string, valueType string) (any, error) {
	switch strings.ToLower(valueType) {
	case "boolean", "bool":
		// bool 三级强转：标准 ParseBool → 数字 "1"/"0" → 浮点非零为 true
		if b, err := strconv.ParseBool(raw); err == nil {
			return b, nil
		}
		if raw == "1" {
			return true, nil
		}
		if raw == "0" {
			return false, nil
		}
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return f != 0, nil
		}
		return nil, fmt.Errorf("cannot convert %q to bool", raw)
	case "sbyte":
		// coerceToInt 内置三级回退：数值解析 → bool(1/0) → 浮点截断
		v, err := coerceToInt(raw, 8)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to sbyte: %w", raw, err)
		}
		return int8(v), nil
	case "byte":
		v, err := coerceToUint(raw, 8)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to byte: %w", raw, err)
		}
		return uint8(v), nil
	case "int16":
		v, err := coerceToInt(raw, 16)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to int16: %w", raw, err)
		}
		return int16(v), nil
	case "uint16":
		v, err := coerceToUint(raw, 16)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to uint16: %w", raw, err)
		}
		return uint16(v), nil
	case "int32":
		v, err := coerceToInt(raw, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to int32: %w", raw, err)
		}
		return int32(v), nil
	case "uint32":
		v, err := coerceToUint(raw, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to uint32: %w", raw, err)
		}
		return uint32(v), nil
	case "int64":
		v, err := coerceToInt(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to int64: %w", raw, err)
		}
		return v, nil
	case "uint64":
		v, err := coerceToUint(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to uint64: %w", raw, err)
		}
		return v, nil
	case "float", "float32":
		// float32 二级强转：数值解析 → bool(1.0/0.0)
		v, err := strconv.ParseFloat(raw, 32)
		if err == nil {
			return float32(v), nil
		}
		if b, ok := parseBoolish(raw); ok {
			if b {
				return float32(1), nil
			}
			return float32(0), nil
		}
		return nil, fmt.Errorf("cannot convert %q to float32", raw)
	case "double", "float64":
		// float64 二级强转：数值解析 → bool(1.0/0.0)
		v, err := strconv.ParseFloat(raw, 64)
		if err == nil {
			return v, nil
		}
		if b, ok := parseBoolish(raw); ok {
			if b {
				return float64(1), nil
			}
			return float64(0), nil
		}
		return nil, fmt.Errorf("cannot convert %q to float64", raw)
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
