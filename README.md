# go-opcua-connector

高性能 OPC UA 到 NATS.io 数据采集连接器，支持单机十万级点位采集。采用内存池架构，支持即时/定时双模式推送，具备完善的数据质量判定和异常兜底机制。

## 项目简介

从 OPC UA Server（如 KepServer）采集工业数据，通过内存池统一管理，灵活选择推送方式后经 NATS.io 消息队列分发。

### 核心特性

| 特性 | 说明 |
|------|------|
| **内存池架构** | `nodeStates` 作为唯一数据源，统一存储节点值、质量、时间戳、状态 |
| **双模式推送** | 即时模式（实时推送变化）+ 定时模式（批量快照推送） |
| **健康检查** | 合并心跳验证和停滞检测，防止稳态数据被误判 |
| **节点自动展开** | 支持通配符递归展开，灵活配置精确或全量采集 |
| **多 Worker 并发** | 多个 subWorker 并行处理订阅数据 |
| **背压保护** | 有缓冲 Channel + 非阻塞 drop，防止通道阻塞 |
| **优雅关闭** | 捕获 SIGINT/SIGTERM，等待 Worker 全部退出后清理资源 |
| **环境变量覆盖** | 支持通过环境变量覆盖配置文件中的任意字段 |

### 推送模式对比

| 模式 | 配置值 | 适用场景 |
|------|--------|---------|
| **即时模式** | `push_mode: immediate` | 下游需要实时数据；数据量适中 |
| **定时模式** | `push_mode: timed` | 下游需要稳定批量数据；数据量大需攒批 |

---

## 性能目标

| 点位规模 | 推荐配置 |
|----------|----------|
| 10 万点位 | 默认配置即可（10 worker + batch 100 + buffer 10000） |
| 20 万点位 | 增加 `worker_count` 到 20，调大 `batch_size` 到 200，调大 `channel_buffer_size` 到 20000 |

---

## 项目结构

```
go-opcua-connector/
├── cmd/
│   ├── doc.go                  # 入口程序说明（Go 社区标准）
│   └── main.go                 # 应用入口，初始化与生命周期管理
├── internal/
│   ├── collector/
│   │   ├── doc.go              # 采集引擎说明
│   │   └── collector.go        # 采集引擎：内存池、订阅处理、健康检查、双模式推送
│   ├── config/
│   │   ├── doc.go              # 配置模块说明
│   │   └── config.go           # 配置结构体定义与校验
│   ├── model/
│   │   ├── doc.go              # 数据模型说明
│   │   └── datapoint.go        # DataPoint、NATSMessage、NodeDataState、CollectorStats
│   ├── nats/
│   │   ├── client.go              # NATS 客户端：连接管理、单条/批量发布、订阅

│   ├── opcua/
│   │   ├── doc.go              # OPC UA 客户端说明
│   │   └── client.go           # OPC UA 客户端：连接、订阅、读、写、节点展开
│   └── writeback/
│       └── handler.go          # 回写处理器：订阅命令、类型转换、写入 OPC UA
├── pkg/
│   └── pool/
│       ├── doc.go              # 对象池说明
│       └── pool.go             # 泛型对象池（Pool[T]、BytePool、SlicePool[T]）
├── config.yaml                 # 应用配置文件
├── config.yaml.example         # 配置模板（带完整注释）
├── go.mod                      # Go 模块定义
├── go.sum                      # 依赖校验
└── README.md                   # 本文件
```

---

## 目录职责

| 目录 | 职责 |
|------|------|
| `cmd/` | 入口：初始化日志/配置、连接 OPC UA 与 NATS、启动 Collector/Writeback、监听退出信号 |
| `internal/collector/` | 核心采集引擎：内存池管理、订阅处理、健康检查（即心跳验证+停滞检测）、即时/定时双模式推送 |
| `internal/config/` | 配置：结构体定义（OPCUAConfig / NATSConfig / CollectorConfig / WritebackConfig）及 Viper 加载器 |
| `internal/model/` | 数据模型：DataPoint（数据点）、NATSMessage（发布消息）、NodeDataState（节点状态）、CollectorStats（统计信息） |
| `internal/nats/` | NATS 客户端：连接建立、单条/批量 Publish、自动重连、连接状态回调 |
| `internal/opcua/` | OPC UA 客户端：Connect（TCP+Session）、Subscribe（创建订阅与监控项）、Read/ReadAll 批量读取、ResolveNodes 节点展开 |
| `internal/writeback/` | 回写处理器：订阅 NATS 写命令、类型转换、写入 OPC UA、发布结果 |
| `pkg/pool/` | 公共对象池：泛型 Pool[T]、BytePool 字节缓冲池、SlicePool[T] 切片池 |

---

## 快速开始

### 环境要求

- Go 1.21+
- OPC UA Server（如 KepServer）
- NATS Server（可选，未连接时仅 WARN 日志，不会阻塞采集）

### 1. 配置文件

编辑项目根目录下的 `config.yaml`：

```yaml
app_name: go-opcua-connector
log_level: info

opcua:
  endpoint: opc.tcp://localhost:49320
  certificate: ""
  private_key: ""
  username: ""
  password: ""
  security_policy: None
  security_mode: None
  request_timeout: 30

nats:
  urls: nats://localhost:4222
  username: ""
  password: ""
  max_reconnects: -1
  reconnect_wait_ms: 1000

collector:
  worker_count: 10
  batch_size: 100
  channel_buffer_size: 10000
  publish_timeout_ms: 5000
  subscription_topic: opcua/data
  monitor_interval_sec: 60
  stale_threshold_sec: 30
  heartbeat_interval_sec: 5
  push_mode: timed              # 即时模式或定时模式
  push_interval_sec: 1         # 定时模式推送间隔
  force_heartbeat: true          # 即时模式心跳强制推送
  subscription_nodes:
    - ns=2;s=t1.d1
```

### 2. 编译

```powershell
# Windows PowerShell
go mod tidy
go build -o go-opcua-connector.exe ./cmd/
```

```bash
# Linux / macOS
go mod tidy
go build -o go-opcua-connector ./cmd/
```

### 3. 运行

```bash
./go-opcua-connector
```

按 `Ctrl+C` 触发优雅关闭，程序会等待 Worker 全部退出后清理资源。

---

## 配置说明

### OPC UA

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `endpoint` | string | — | 必填，OPC UA 服务器地址，格式 `opc.tcp://host:port` |
| `certificate` | string | `""` | 客户端 X.509 证书文件路径 |
| `private_key` | string | `""` | 客户端私钥文件路径 |
| `username` | string | `""` | 用户名（匿名认证时留空） |
| `password` | string | `""` | 密码 |
| `security_policy` | string | `None` | 安全策略：`None` / `Basic128Rsa15` / `Basic256` / `Basic256Sha256` |
| `security_mode` | string | `None` | 安全模式：`None` / `Sign` / `SignAndEncrypt` |
| `request_timeout` | int | `30` | 请求超时（秒） |

### NATS

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `urls` | string | — | 必填，NATS 服务器地址，多个用逗号分隔 |
| `username` | string | `""` | 用户名 |
| `password` | string | `""` | 密码 |
| `max_reconnects` | int | `-1` | 最大重连次数，`-1` 表示无限重连 |
| `reconnect_wait_ms` | int | `1000` | 重连等待间隔（毫秒） |

### 采集器

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `worker_count` | int | `10` | 订阅数据处理 Worker 数量 |
| `batch_size` | int | `100` | 每批发布的数据点数 |
| `channel_buffer_size` | int | `10000` | 内部 Channel 缓冲区大小 |
| `publish_timeout_ms` | int | `5000` | 批量发布超时（毫秒） |
| `subscription_topic` | string | `opcua/data` | NATS 发布主题 |
| `monitor_interval_sec` | int | `60` | 统计日志输出间隔（秒） |
| `stale_threshold_sec` | int | `30` | 数据停滞判定阈值：超过此秒数未收到更新的标记为 Stale |
| `heartbeat_interval_sec` | int | `5` | 健康检查间隔（秒） |
| `push_mode` | string | `timed` | 推送模式：`immediate`（即时）或 `timed`（定时） |
| `push_interval_sec` | int | `1` | 定时模式推送间隔（秒） |
| `force_heartbeat` | bool | `true` | 即时模式下心跳是否强制推送 |
| `subscription_nodes` | []string | — | OPC UA 节点 ID 列表，支持通配符递归展开 |

### 回写

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `write_subject` | string | `opcua/write` | 接收回写命令的 NATS 主题 |
| `result_subject` | string | `opcua/write/result` | 发布回写结果的 NATS 主题 |

---

## 环境变量覆盖

支持通过环境变量覆盖配置文件中的任意字段，将 YAML 层级以 `_` 连接并转为大写：

```powershell
# Windows PowerShell
$env:OPCUA_ENDPOINT = "opc.tcp://10.0.0.1:4840"
$env:NATS_URLS = "nats://10.0.0.2:4222"
$env:COLLECTOR_PUSH_MODE = "immediate"
$env:COLLECTOR_WORKER_COUNT = "20"
```

```bash
# Linux / macOS
export OPCUA_ENDPOINT="opc.tcp://10.0.0.1:4840"
export NATS_URLS="nats://10.0.0.2:4222"
export COLLECTOR_PUSH_MODE="immediate"
export COLLECTOR_WORKER_COUNT="20"
```

---

## 架构设计

### 内存池 + 双模式推送架构

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│     OPC UA      │ ──→  │   Collector     │ ──→  │     NATS        │
│   (KepServer)   │      │  (内存池架构)    │      │   (消息队列)    │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │     nodeStates        │
                    │   (内存池 - 唯一数据源) │
                    └───────────────────────┘
```

### 数据流

```
OPC UA 订阅回调
      ↓
handleDataPoint() → 更新内存池 (nodeStates)
      ↓
┌─────────────────┬─────────────────┐
↓                 ↓                 ↓
即时模式          定时模式          健康检查
立即推送          标记 Dirty        (healthCheckWorker)
      ↓                 ↓                 ↓
pubCh ──→ publishWorker ──→ NATS.io
```

### 推送模式

#### 即时模式 (`push_mode: immediate`)

- OPC UA 推送新数据 → 更新内存池 → **立即**推送到 NATS
- 心跳验证成功后，若 `force_heartbeat=true` 也强制推送当前值
- 适用场景：下游需要实时数据

#### 定时模式 (`push_mode: timed`)

- OPC UA 推送新数据 → 更新内存池 → **标记 Dirty**
- 定时器周期性扫描所有脏节点，推送**全量快照**
- 适用场景：下游需要稳定批量数据；数据量大需攒批

### 四条协程

| 协程 | 数量 | 触发机制 | 职责 |
|------|------|---------|------|
| `subWorker` | N（可配） | 监听 subCh 通道 | 接收订阅数据，更新内存池，即时模式立即推送 |
| `healthCheckWorker` | 1 | 每 `heartbeat_interval_sec` 秒 | 批量拉取节点数据验证可达性 + 停滞检测 |
| `publishWorkerImmediate` | 1 | 监听 pubCh 通道 | 即时模式：逐点发布到 NATS |
| `publishWorkerTimed` | 1 | 每 `push_interval_sec` 秒 | 定时模式：批量推送所有脏节点的全量快照 |
| `monitorStats` | 1 | 每 `monitor_interval_sec` 秒 | 统计信息输出 |

### 健康检查逻辑

`healthCheckWorker` 合并了心跳验证和停滞检测：

1. **心跳验证**：主动 ReadAll 批量拉取全部节点
   - 若 `Quality == Good`：更新内存池，即时模式下强制推送
   - 若 `Quality != Good`：仅更新 Quality，不更新 Timestamp，等待停滞检测

2. **停滞检测**：检查 Timestamp 超过 `stale_threshold_sec` 的节点
   - 标记为 `Status = "Stale"`
   - 即时模式立即推送；定时模式标记 Dirty

---

## 节点自动展开

`subscription_nodes` 支持两种模式：

### 精确模式（无通配符）

配置文件夹节点，启动时 Browse 该节点下的所有 Variable 类型子节点：

```
配置: ns=2;s=t1.d1（文件夹）
    ↓ Browse
ns=2;s=t1.d1.t1, ns=2;s=t1.d1.t2, ...（叶子变量）
```

### 通配模式（有 `.*` 后缀）

递归穿透所有子文件夹，收集全部 Variable 叶子节点：

```
配置: ns=2;s=t1.d1.*（递归通配）
    ↓ Browse + 递归
t1.d1 下所有层级、所有文件夹的叶子变量
```

适用场景：KepServer 有 `Channel.Device.标记组.Tag` 多级结构时使用。

---

## 数据质量判定

| Quality | 含义 | 触发条件 |
|---------|------|---------|
| `Good` | 数据正常 | StatusCode 高位为 0x00 |
| `Bad` | 数据无效 | StatusCode 高位为 0x80 |
| `Uncertain` | 数据质量无法保证 | StatusCode 高位为 0x40 |
| `Stale` | 数据停滞 | 超过 `stale_threshold_sec` 未收到更新 |
| `BadNoValue` | 无值通知 | 收到通知但 Value 字段为 nil |

---

## NATS 消息格式

### 数据发布

定时模式（`push_mode: timed`）每个周期推送全量快照，即时模式（`push_mode: immediate`）每次变化推单条，两种模式都使用 `PublishBatch` 统一通道，格式为批量包装：

```json
{
  "topic": "opcua/data",
  "points": [
    {
      "node_id": "ns=2;s=t1.d1.t495",
      "value": 42.5,
      "quality": "Good",
      "timestamp": "2026-05-12T10:00:00Z",
      "topic": "opcua/data"
    }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `topic` | string | 外层 NATS 主题，受 `subscription_topic` 控制 |
| `points` | array | DataPoint 数组，定时模式包含全量节点，即时模式包含单个变化节点 |
| `points[].node_id` | string | OPC UA 节点标识符 |
| `points[].value` | any | 节点当前值 |
| `points[].quality` | string | 品质：`Good` / `Bad` / `Uncertain` / `Stale` |
| `points[].timestamp` | string | ISO 8601 时间戳 |
| `points[].topic` | string | 内层数据来源主题，与外层 `topic` 相同 |

### 回写命令

```json
{
  "node_id": "ns=2;s=t1.d1.Temperature",
  "value": "25.5",
  "value_type": "float64",
  "request_id": "abc123"
}
```

### 回写结果

```json
{
  "node_id": "ns=2;s=t1.d1.Temperature",
  "success": true,
  "written_value": 25.5,
  "request_id": "abc123"
}
```

---

## 异常兜底设计

### 采集阶段

| 异常 | 策略 |
|------|------|
| 节点 ID 不合法 | Warn + 跳过，不影响其他节点 |
| Browse 失败 | Warn + 保留原始节点直接订阅 |
| 订阅通知 Error | Log Error + continue |
| 通知 Value 为 nil | 构造 Quality="BadNoValue" 数据点 |
| ReadAll 失败 | 不更新 Timestamp，等待下一次心跳 |

### 停滞检测

| 场景 | 策略 |
|------|------|
| 订阅推送停止 + 心跳 Good | 心跳刷新 Timestamp，不发 Stale |
| 订阅推送停止 + 心跳 Bad | 仅更新 Quality，等待过阈值后发 Stale |
| 新节点初始化 | Timestamp 设为当前时间 |

---

## 性能调优

### 调参指南

| 参数 | 作用 | 建议 |
|------|------|------|
| `worker_count` | 提升订阅数据处理并发能力 | 10 万点位：10-20 |
| `channel_buffer_size` | 应对更大突发流量 | 应大于 batch_size × worker_count |
| `heartbeat_interval_sec` | 健康检查频率 | 数据量大可适当增大 |
| `stale_threshold_sec` | 停滞判定阈值 | 应大于 heartbeat_interval_sec |
| `push_interval_sec` | 定时模式推送间隔 | 仅 timed 模式生效 |

---

## 项目阅读指南

建议按依赖关系从底层到上层阅读：

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `cmd/main.go` | main() 启动流程 |
| 2 | `internal/model/datapoint.go` | DataPoint、NATSMessage、NodeDataState、CollectorStats |
| 3 | `internal/config/config.go` | 配置结构体定义 |
| 4 | `internal/opcua/client.go` | Connect() → Subscribe() → handleNotifications() → ResolveNodes() → ReadAll() |
| 5 | `internal/nats/client.go` | Connect() → Publish() → PublishBatch() |
| 6 | `internal/collector/collector.go` | Start() → subWorker() → healthCheckWorker() → publishWorker() |
| 7 | `internal/writeback/handler.go` | 订阅 NATS 命令 → 类型转换 → 写入 OPC UA |

---

## 常用命令

```powershell
# 开发调试
go run ./cmd/

# 编译检查（提交前必跑）
go build ./...

# 静态检查（提交前必跑）
go vet ./...

# 格式化代码
go fmt ./...

# 清理依赖
go mod tidy

# 跨平台编译
$env:GOOS="linux"; $env:GOARCH="amd64"; go build -o go-opcua-connector ./cmd/
```

---

## 性能分析（pprof）

程序内置 pprof 端点，监听 `:6060`，用于排查内存泄漏、goroutine 泄漏等运行时问题。

### 启动后访问

```bash
# 浏览器概览
http://localhost:6060/debug/pprof/

# 抓 heap 快照（推荐运行半小时后抓）
go tool pprof http://localhost:6060/debug/pprof/heap

# 抓 goroutine 快照
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

### pprof 交互命令

```
top30                # 按内存占用排名前 30
list <funcName>      # 查看某函数逐行分配详情
web                  # 生成火焰图（需安装 graphviz）
tree                 # 调用树视图
```

### 对比两次 heap 找增量泄漏

```bash
# 第一次抓取（运行一段时间后）
curl -o heap1.pb.gz http://localhost:6060/debug/pprof/heap

# 等待 10 分钟后第二次抓取
curl -o heap2.pb.gz http://localhost:6060/debug/pprof/heap

# 对比差异
go tool pprof -base heap1.pb.gz heap2.pb.gz
```

### 关注指标

| 指标 | 含义 |
|------|------|
| `inuse_space` | 当前实际占用的堆内存 |
| `alloc_space` | 累计分配总量（含已释放） |
| `alloc_space` ↑ 但 `inuse_space` 稳定 | GC 慢但不泄漏 |
| `inuse_space` 持续 ↑ | 真内存泄漏 |

---

## 依赖

| 库 | 版本 | 用途 |
|----|------|------|
| `github.com/gopcua/opcua` | v0.6.1 | OPC UA 客户端 |
| `github.com/nats-io/nats.go` | v1.37.0 | NATS 消息队列客户端 |
| `github.com/spf13/viper` | v1.19.0 | 配置加载（YAML + 环境变量） |
| `go.uber.org/zap` | v1.27.0 | 高性能结构化日志 |

## License

MIT
