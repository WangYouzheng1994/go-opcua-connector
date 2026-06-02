# go-opcua-connector MQTT 输出改造记录

---

## 一、改造目标

为 go-opcua-connector 新增 MQTT 输出能力。改造后使用者通过配置一个字段即可切换数据输出目标：

```yaml
collector:
  output_type: nats   # 推送到 NATS（默认，向后兼容）
  output_type: mqtt   # 推送到 MQTT
```

---

## 二、涉及文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `internal/collector/publisher.go` | **新建** | `Publisher` 接口 |
| `internal/writeback/transport.go` | **新建** | `Transport` 接口 + `Subscription` 接口 |
| `internal/writeback/engine.go` | **新建** | 回写引擎（从 handler.go 提取的纯业务逻辑） |
| `internal/mqtt/client.go` | **新建** | MQTT 客户端，实现 Publisher + Transport |
| `internal/writeback/handler.go` | **删除** | 功能迁移到 engine.go |
| `internal/config/config.go` | 修改 | 新增 `OutputType`、`MQTTConfig`，Validate 按 output_type 分支校验 |
| `internal/config/loader.go` | 修改 | 新增 MQTT 默认值 |
| `internal/nats/client.go` | 修改 | 删除冗余统计代码 + Context 响应 + 实现 Transport 接口 |
| `internal/collector/collector.go` | 修改 | `natsClient` → `publisher Publisher`，移除 NATS 硬依赖 |
| `internal/model/datapoint.go` | 修改 | 删除 DataPoint.Topic，新增 BatchMessage/BatchPoint 序列化结构 |
| `internal/opcua/client.go` | 修改 | 删除 DataPoint.Topic 赋值 |
| `cmd/main.go` | 修改 | 按 output_type 组装 Publisher 和 Transport |
| `config.yaml` | 修改 | 新增 `output_type` + `mqtt` 配置节 |
| `config.yaml.example` | 修改 | 同步更新配置模板和注释 |
| `go.mod` | 修改 | 新增 `github.com/eclipse/paho.mqtt.golang` 依赖 |

---

## 三、架构变更

### 3.1 改造前

```
Collector ──→ nats.Client (硬编码)
Writeback ──→ nats.Client (硬编码)
```

### 3.2 改造后

```
config.yaml
  collector.output_type: nats | mqtt
              │
              ▼
          main.go ── 一个 switch 决定用哪种客户端
              │
    ┌─────────┴─────────┐
    ▼                   ▼
Collector           Writeback.Engine
    │                   │
    │ Publisher 接口    │ Transport 接口
    ▼                   ▼
  nats.Client  or  mqtt.Client
  (同一个客户端同时实现两个接口)
```

### 3.3 核心接口

```go
// Publisher — Collector 数据输出
type Publisher interface {
    PublishBatch(ctx context.Context, topic string, points []model.DataPoint) error
    IsConnected() bool
    Close()
}

// Transport — Writeback 传输层
type Transport interface {
    Subscribe(ctx context.Context, subject string, handler func(data []byte)) (Subscription, error)
    PublishResult(ctx context.Context, subject string, data []byte) error
    Close()
}
```

`nats.Client` 和 `mqtt.Client` 都同时实现这两个接口，同一个连接被 Collector 和 Writeback 共享。

---

## 四、关键改动详解

### 4.1 Context 真正生效

**问题**：`PublishBatch(ctx, ...)` 接收 ctx 参数但底层调用不检测取消/超时。

**修复**：所有 Publish 操作改为 goroutine + select 模式：

```go
done := make(chan error, 1)
go func() {
    done <- client.Publish(...)  // 阻塞操作放 goroutine
}()
select {
case err := <-done:
    return err
case <-ctx.Done():
    return ctx.Err()             // ctx 超时/取消时生效
}
```

`chan error, 1` 的 buffer=1 是关键——外层 select 命中 `ctx.Done()` 后，goroutine 仍会向 done 写入，buffer 保证 goroutine 不泄漏。

### 4.2 Writeback 解耦

**问题**：`writeback.Handler` 处处引用 `*nats.Client`、`*nats.Subscription`、`*nats.Msg`，完全绑定 NATS。

**修复**：拆分为 Transport 接口 + Engine 业务逻辑。

- `writeback.Transport` — 抽象收/发能力（Subscribe + PublishResult）
- `writeback.Engine` — 纯业务逻辑（JSON解析 → 类型转换 → OPC UA写入），只依赖 Transport 接口
- 原 `handler.go` 中的 `handleMessage`、`typeGroup`、`isCompatibleTypeGroup`、`publishResult` 原样搬入 Engine

### 4.3 统计信息统一

**问题**：NATS Client 和 Collector 各自维护成功/失败计数，且 NATS Client 的计数器无人读取（死代码）。

**修复**：从 nats.Client 删除 `failCount`、`successCount`、`GetStats()`，mqtt.Client 不写统计代码。统计统一由 Collector 管理。

### 4.4 DataPoint.Topic 移除

**问题**：每条 DataPoint 内嵌 `Topic` 字段，但批量消息顶层已有 topic，点级 topic 冗余。且 MQTT 模式下 DataPoint.topic 来自 collector.subscription_topic，与实际发布目标 mqtt.topic 不一致。

**修复**：从 DataPoint 删除 `Topic` 字段。批量消息的 topic 由 PublishBatch 内部根据实际发布目标设置。

### 4.5 输出格式统一

**改造后输出格式**（NATS 和 MQTT 共用）：

```json
{
  "timestamp": 1780389206931,
  "values": [
    {
      "id": "t1.d1.t4",
      "v": 0,
      "q": true,
      "t": 1780389206686
    }
  ]
}
```

| 字段 | 来源 | 说明 |
|------|------|------|
| `timestamp` | `time.Now().UnixMilli()` | 批量消息生成时间 |
| `values[].id` | DataPoint.NodeID 去 `ns=N;s=` 前缀 | 简短节点标识 |
| `values[].v` | DataPoint.Value | 数据值 |
| `values[].q` | DataPoint.Quality == "Good" | bool 品质 |
| `values[].t` | DataPoint.Timestamp.UnixMilli() | 数据时间戳 |

序列化逻辑统一在 [model/datapoint.go](file:///e:/ls/ProjectCollection/go-opcua-connector/internal/model/datapoint.go) 的 `NewBatchMessage()` 函数中，NATS 和 MQTT 共用。

### 4.6 非叶子节点过滤

**问题**：模糊匹配（`.*`）展开时，文件夹节点若无 Variable 子节点会被当做叶子节点订阅，推送 `v:null, q:Bad` 的无效数据。

**修复**：
- `handleDataPoint` 入口：Quality 以 "Bad" 开头且 Value == nil 的数据点直接丢弃
- `pushDirtyNodes` 遍历：同理跳过 Bad+null 的节点

---

## 五、MQTT 配置说明

```yaml
collector:
  output_type: mqtt   # 切换为 MQTT

mqtt:
  brokers: tcp://localhost:1883      # Broker 地址，多个逗号分隔
  client_id: ""                      # 空则自动生成
  username: ""                       # Broker 认证
  password: ""
  topic: opcua/data                  # 发布主题，默认 opcua/data
  qos: 0                             # 0=最多一次 1=至少一次 2=恰好一次
  retained: false                    # 是否保留消息
  clean_session: true
  keepalive_sec: 60                  # 心跳间隔
  connect_timeout_sec: 10
  publish_timeout_sec: 5
  auto_reconnect: true
  max_reconnect_delay_sec: 60
  insecure_skip_verify: false        # TLS 跳过验证（仅测试）
```

---

## 六、扩展性

未来接入 Kafka / RabbitMQ / Pulsar 等中间件时：

1. `internal/config/config.go` 加 `OutputTypeKafka` 常量和对应 Config 结构体
2. 新建 `internal/kafka/client.go`，实现 `Publisher` + `Transport` 接口
3. `cmd/main.go` 的 switch 加一个 case
4. Collector、Writeback 核心逻辑一行不动
