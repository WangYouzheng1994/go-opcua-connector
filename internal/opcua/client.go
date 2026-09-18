// Package opcua provides OPC UA connection, acquisition adapter, and writeback support.
package opcua

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"go-opcua-connector/internal/config"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

// Client OPC UA客户端管理器，负责连接、协议侧采集资源和写回。
type Client struct {
	// config OPC UA配置
	config *config.OPCUAConfig
	// client gopcua底层客户端
	client *opcua.Client
	// logger 日志记录器
	logger *zap.Logger
	// mu 读写锁，保护连接状态和客户端实例
	mu sync.RWMutex
	// identityMu 保护 PointID 与 SourceRef 的双向索引
	identityMu sync.RWMutex
	// sourceByPointID 仅供 OPC UA Adapter 内部协议操作使用
	sourceByPointID map[string]SourceRef
	// pointIDBySource 用于发现去重和协议通知反查
	pointIDBySource map[string]string
	// browsePathByPointID 保存生成 PointID 时使用的完整 BrowsePath
	browsePathByPointID map[string][]string
	// adapterBackend 允许 Task 3 的协议操作使用可测试后端；生产环境为空时使用 client。
	adapterBackend acquisitionBackend
	// writeTargetBackend 允许 PointID 回写目标使用可测试后端；生产环境为空时使用 client。
	writeTargetBackend writeTargetBackend
	// handleMu 保护当前连接内单调递增的 ClientHandle。
	handleMu         sync.Mutex
	nextClientHandle uint64
	// adapterNow 为订阅接收时间和主动读取完成时间提供时钟。
	adapterNow func() time.Time
}

// NewClient 创建新的OPC UA客户端
func NewClient(cfg *config.OPCUAConfig, logger *zap.Logger) *Client {
	return &Client{
		config:              cfg,
		logger:              logger,
		sourceByPointID:     make(map[string]SourceRef),
		pointIDBySource:     make(map[string]string),
		browsePathByPointID: make(map[string][]string),
		nextClientHandle:    1,
		adapterNow:          time.Now,
	}
}

// DiscoverPoints 根据 OPC UA BrowsePath 生成稳定 PointID，并原子替换协议侧身份映射。
func (c *Client) DiscoverPoints(ctx context.Context, configuredNodes []string) (PointDiscoveryResult, error) {
	return c.discoverPoints(ctx, configuredNodes, false)
}

// DiscoverAdditionalPoints 只浏览此前失败的配置项，并把成功结果合并到现有身份映射。
func (c *Client) DiscoverAdditionalPoints(ctx context.Context, configuredNodes []string) (PointDiscoveryResult, error) {
	return c.discoverPoints(ctx, configuredNodes, true)
}

func (c *Client) discoverPoints(ctx context.Context, configuredNodes []string, mergeExisting bool) (PointDiscoveryResult, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return PointDiscoveryResult{}, fmt.Errorf("client not connected")
	}

	overrides := make([]pointIDOverride, 0, len(c.config.PointIDOverrides))
	for _, override := range c.config.PointIDOverrides {
		overrides = append(overrides, pointIDOverride{sourceRef: override.SourceRef, pointID: override.PointID})
	}
	discoverer, err := newPointDiscoverer(&opcuaBrowseService{client: client}, overrides)
	if err != nil {
		return PointDiscoveryResult{}, err
	}
	result, registry := discoverer.discover(ctx, configuredNodes)
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if mergeExisting {
		registry, result = c.mergePointRegistry(registry, result)
	}

	c.installPointRegistry(registry)
	return result, nil
}

func (c *Client) mergePointRegistry(addition pointRegistry, result PointDiscoveryResult) (pointRegistry, PointDiscoveryResult) {
	c.identityMu.RLock()
	merged := pointRegistry{
		sourceByPointID:     make(map[string]SourceRef, len(c.sourceByPointID)+len(addition.sourceByPointID)),
		pointIDBySource:     make(map[string]string, len(c.pointIDBySource)+len(addition.pointIDBySource)),
		browsePathByPointID: make(map[string][]string, len(c.browsePathByPointID)+len(addition.browsePathByPointID)),
	}
	for pointID, sourceRef := range c.sourceByPointID {
		merged.sourceByPointID[pointID] = sourceRef
	}
	for sourceRef, pointID := range c.pointIDBySource {
		merged.pointIDBySource[sourceRef] = pointID
	}
	for pointID, browsePath := range c.browsePathByPointID {
		merged.browsePathByPointID[pointID] = append([]string(nil), browsePath...)
	}
	c.identityMu.RUnlock()

	accepted := make([]DiscoveredPoint, 0, len(result.Points))
	for _, point := range result.Points {
		pointID := point.PointID
		sourceRef, exists := addition.sourceByPointID[pointID]
		if !exists {
			continue
		}
		if existingSource, exists := merged.sourceByPointID[pointID]; exists && existingSource.String() != sourceRef.String() {
			result.Issues = append(result.Issues, DiscoveryIssue{Source: sourceRef.String(), Reason: "PointID conflicts with an existing SourceRef"})
			continue
		}
		if existingPointID, exists := merged.pointIDBySource[sourceRef.String()]; exists && existingPointID != pointID {
			result.Issues = append(result.Issues, DiscoveryIssue{Source: sourceRef.String(), Reason: "SourceRef conflicts with an existing PointID"})
			continue
		}
		merged.sourceByPointID[pointID] = sourceRef
		merged.pointIDBySource[sourceRef.String()] = pointID
		merged.browsePathByPointID[pointID] = append([]string(nil), addition.browsePathByPointID[pointID]...)
		accepted = append(accepted, point)
	}
	result.Points = accepted
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].Source == result.Issues[j].Source {
			return result.Issues[i].Reason < result.Issues[j].Reason
		}
		return result.Issues[i].Source < result.Issues[j].Source
	})
	return merged, result
}

func (c *Client) installPointRegistry(registry pointRegistry) {
	c.identityMu.Lock()
	c.sourceByPointID = registry.sourceByPointID
	c.pointIDBySource = registry.pointIDBySource
	c.browsePathByPointID = registry.browsePathByPointID
	c.identityMu.Unlock()
}

func (c *Client) resolveSourceRef(pointID string) (SourceRef, bool) {
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	sourceRef, ok := c.sourceByPointID[pointID]
	return sourceRef, ok
}

func (c *Client) resolvePointID(sourceRef SourceRef) (string, bool) {
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	pointID, ok := c.pointIDBySource[sourceRef.String()]
	return pointID, ok
}

func (c *Client) resolveBrowsePath(pointID string) ([]string, bool) {
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	path, ok := c.browsePathByPointID[pointID]
	return append([]string(nil), path...), ok
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
	c.resetClientHandles()

	c.logger.Info("Connected to OPC UA server", zap.String("endpoint", endpoint))
	return nil
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
	return c.ConnectionState() == opcua.Connected
}

// ConnectionState 返回底层 gopcua 客户端的真实连接状态。
func (c *Client) ConnectionState() opcua.ConnState {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return opcua.Closed
	}
	return client.State()
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
