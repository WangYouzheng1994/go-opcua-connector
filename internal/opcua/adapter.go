package opcua

import (
	"context"
	"errors"
	"sync"

	"go-opcua-connector/internal/collector"

	gopcua "github.com/gopcua/opcua"
)

// AcquisitionAdapter 将 OPC UA Client 暴露为协议无关采集边界。
// 生产入口将在后续 Task 切换到该 Adapter。
type AcquisitionAdapter struct {
	client          *Client
	configuredNodes []string
	mu              sync.Mutex
	generation      uint64
	pendingNodes    []string
}

// NewAcquisitionAdapter 创建 OPC UA 采集 Adapter。
func NewAcquisitionAdapter(client *Client, configuredNodes []string) *AcquisitionAdapter {
	return &AcquisitionAdapter{client: client, configuredNodes: append([]string(nil), configuredNodes...)}
}

func (a *AcquisitionAdapter) Discover(ctx context.Context, connectionGeneration uint64) collector.DiscoveryResult {
	if a.client == nil {
		return collector.DiscoveryResult{Issues: []collector.DiscoveryIssue{{Reason: "OPC UA client is required"}}}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	nodes := a.pendingNodes
	fullDiscovery := a.generation == 0 || a.generation != connectionGeneration
	if fullDiscovery {
		nodes = a.configuredNodes
	}
	if len(nodes) == 0 {
		return collector.DiscoveryResult{}
	}
	var result PointDiscoveryResult
	var err error
	if fullDiscovery {
		result, err = a.client.DiscoverPoints(ctx, nodes)
	} else {
		result, err = a.client.DiscoverAdditionalPoints(ctx, nodes)
	}
	a.generation = connectionGeneration
	a.pendingNodes = failedConfiguredNodes(nodes, result.Issues)
	return convertDiscoveryResult(result, err)
}

// Rediscover 始终 Browse 全部配置点，并用本次结果替换协议侧身份映射。
func (a *AcquisitionAdapter) Rediscover(ctx context.Context, connectionGeneration uint64) collector.DiscoveryResult {
	if a.client == nil {
		return collector.DiscoveryResult{Issues: []collector.DiscoveryIssue{{Reason: "OPC UA client is required"}}}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	result, err := a.client.DiscoverPoints(ctx, a.configuredNodes)
	a.generation = connectionGeneration
	a.pendingNodes = failedConfiguredNodes(a.configuredNodes, result.Issues)
	return convertDiscoveryResult(result, err)
}

func convertDiscoveryResult(result PointDiscoveryResult, err error) collector.DiscoveryResult {
	converted := collector.DiscoveryResult{
		Points: make([]collector.PointID, 0, len(result.Points)),
		Issues: make([]collector.DiscoveryIssue, 0, len(result.Issues)+1),
	}
	for _, point := range result.Points {
		converted.Points = append(converted.Points, collector.PointID(point.PointID))
	}
	for _, issue := range result.Issues {
		converted.Issues = append(converted.Issues, collector.DiscoveryIssue{Source: issue.Source, Reason: issue.Reason})
	}
	if err != nil {
		converted.Issues = append(converted.Issues, collector.DiscoveryIssue{Reason: err.Error()})
	}
	return converted
}

func failedConfiguredNodes(configuredNodes []string, issues []DiscoveryIssue) []string {
	failed := make(map[string]struct{}, len(issues))
	for _, issue := range issues {
		failed[issue.Source] = struct{}{}
	}
	result := make([]string, 0, len(failed))
	for _, configuredNode := range configuredNodes {
		if _, exists := failed[configuredNode]; exists {
			result = append(result, configuredNode)
		}
	}
	return result
}

func (a *AcquisitionAdapter) Subscribe(
	ctx context.Context,
	points []collector.PointID,
	tokenByPoint map[collector.PointID]collector.GenerationToken,
	handler collector.SampleHandler,
) collector.SubscriptionResult {
	if a.client == nil {
		failed := make([]collector.PointFailure, 0, len(points))
		for _, pointID := range points {
			failed = append(failed, collector.PointFailure{PointID: pointID, Reason: "OPC UA client is required"})
		}
		return collector.SubscriptionResult{Failed: failed}
	}
	return a.client.SubscribePoints(ctx, points, tokenByPoint, handler)
}

func (a *AcquisitionAdapter) Verify(
	ctx context.Context,
	points []collector.PointID,
	tokenByPoint map[collector.PointID]collector.GenerationToken,
) collector.VerificationResult {
	if a.client == nil {
		return collector.VerificationResult{RequestFailure: "OPC UA client is required"}
	}
	return a.client.ReadInitial(ctx, points, tokenByPoint)
}

func (a *AcquisitionAdapter) ConnectionState() collector.SourceConnectionState {
	if a.client == nil {
		return collector.SourceDisconnected
	}
	switch a.client.ConnectionState() {
	case gopcua.Connected:
		return collector.SourceConnected
	case gopcua.Connecting:
		return collector.SourceConnecting
	case gopcua.Reconnecting:
		return collector.SourceReconnecting
	default:
		return collector.SourceDisconnected
	}
}

func (a *AcquisitionAdapter) Close(context.Context) error {
	if a.client == nil {
		return errors.New("OPC UA client is required")
	}
	a.client.Close()
	return nil
}

var _ collector.SourceAdapter = (*AcquisitionAdapter)(nil)
