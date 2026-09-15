package collector

import "context"

// PointFailure 是不包含数据源协议字段的单点操作诊断。
type PointFailure struct {
	PointID PointID
	Reason  string
}

// PointSubscription 管理一次订阅操作创建的协议资源和通知转发。
type PointSubscription interface {
	Remove(ctx context.Context, pointID PointID) error
	Close(ctx context.Context) error
}

// SubscriptionResult 返回逐点订阅结果；部分失败不会丢弃成功点。
type SubscriptionResult struct {
	Succeeded    []PointID
	Failed       []PointFailure
	Subscription PointSubscription
}

// VerificationResult 返回主动读取的逐点标准化结果和请求级失败。
type VerificationResult struct {
	Verifications  []Verification
	Failed         []PointFailure
	RequestFailure string
}

// SampleHandler 接收数据源 Adapter 标准化后的订阅样本。
type SampleHandler func(sample PointSample)

// DiscoveryIssue 是协议无关的发现诊断。
type DiscoveryIssue struct {
	Source string
	Reason string
}

// DiscoveryResult 返回本次发现得到的点位及诊断。
type DiscoveryResult struct {
	Points []PointID
	Issues []DiscoveryIssue
}

// SourceConnectionState 表示 Adapter 当前观察到的连接状态。
type SourceConnectionState string

const (
	SourceConnected    SourceConnectionState = "Connected"
	SourceConnecting   SourceConnectionState = "Connecting"
	SourceDisconnected SourceConnectionState = "Disconnected"
	SourceReconnecting SourceConnectionState = "Reconnecting"
)

// SourceAdapter 是当前采集协调器依赖的协议无关边界。
// 它服务于本期采集重构，不承诺作为所有未来数据源协议的统一接口。
type SourceAdapter interface {
	Discover(ctx context.Context, connectionGeneration uint64) DiscoveryResult
	// Rediscover 强制重新发现全部配置点，用于连接恢复后重建协议侧身份映射。
	Rediscover(ctx context.Context, connectionGeneration uint64) DiscoveryResult
	Subscribe(ctx context.Context, points []PointID, tokenByPoint map[PointID]GenerationToken, handler SampleHandler) SubscriptionResult
	Verify(ctx context.Context, points []PointID, tokenByPoint map[PointID]GenerationToken) VerificationResult
	ConnectionState() SourceConnectionState
	Close(ctx context.Context) error
}
