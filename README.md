# go-opcua-connector

高性能 OPC UA 到 NATS.io 数据采集连接器，支持单机十万级点位采集。采用订阅+拉取复合模式，具备完善的数据质量判定和异常兜底机制。

## 项目简介

从 OPC UA Server（如 KepServer）采集工业数据，经 Quality 判定后通过 NATS.io 消息队列统一分发。

### 核心特性

| 特性 | 说明 |
|------|------|
| **订阅 + 拉取复合模式** | OPC UA 订阅实时推送 + 定期主动拉取心跳验证，双重保障数据可靠性 |
| **节点自动展开** | 配置文件夹节点自动 Browse，展开为全部叶子变量节点 |
| **三线一道防线** | 第一道：订阅实时推送；第二道：心跳拉取验证；第三道：停滞检测标记 Stale |
| **多 Worker 并发** | 多个 subWorker 并行处理订阅数据，分散 CPU 压力 |
| **批量发布** | 攒批后统一提交 NATS，减少网络往返开销 |
| **背压保护** | 有缓冲 Channel + 非阻塞 drop，防止订阅通道被阻塞 |
| **优雅关闭** | 捕获 SIGINT/SIGTERM，30 秒超时窗口，确保已处理批次全部完成 |
| **环境变量覆盖** | 支持通过环境变量覆盖配置文件中的任意字段 |

### 数据质量判定

通过订阅（实时）和拉取（验证）双重机制，结合 OPC UA StatusCode 进行统一判定：

| Quality | 含义 | 触发条件 |
|---------|------|---------|
| `Good` | 数据正常 | StatusCode 高位为 0x00 |
| `Bad` | 数据无效 | StatusCode 高位为 0x80 |
| `Uncertain` | 数据质量无法保证 | StatusCode 高位为 0x40 |
| `Stale` | 数据停滞 | 超过 `stale_threshold_sec` 未收到更新 |
| `BadNoValue` | 无值通知 | 收到通知但 Value 字段为 nil |

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
│   │   └── collector.go        # 采集引擎：订阅处理、心跳验证、停滞检测、批量发布
│   ├── config/
│   │   ├── doc.go              # 配置模块说明
│   │   ├── config.go           # 配置结构体定义与校验
│   │   └── loader.go           # Viper 配置加载器（YAML + 环境变量覆盖）
│   ├── model/
│   │   ├── doc.go              # 数据模型说明
│   │   └── datapoint.go        # DataPoint、NATSMessage、NodeDataState、CollectorStats
│   ├── nats/
│   │   ├── doc.go              # NATS 发布模块说明
│   │   └── publisher.go        # NATS 发布者：连接管理、单条/批量发布、重连
│   └── opcua/
│       ├── doc.go              # OPC UA 客户端说明
│       └── client.go           # OPC UA 客户端：连接、订阅、Read/ReadAll、节点展开
├── pkg/
│   └── pool/
│       ├── doc.go              # 对象池说明
│       └── pool.go             # 泛型对象池（Pool[T]、BytePool、SlicePool[T]）
├── config.yaml                 # 应用配置文件
├── go.mod                      # Go 模块定义
├── go.sum                      # 依赖校验
└── README.md                   # 本文件
```

> 注：每个包目录下的 `doc.go` 遵循 Go 社区 `// Package xxx ...` 标准，通过 `go doc ./internal/xxx` 即可查看包说明。

---

## 目录职责

| 目录 | 职责 |
|------|------|
| `cmd/` | 入口：初始化日志/配置、连接 OPC UA 与 NATS、启动 Collector、监听退出信号 |
| `internal/collector/` | 核心采集引擎：启动 4 类协程（subWorker×N / heartbeatWorker / staleCheckWorker / publishWorker），通过管道串联数据流 |
| `internal/config/` | 配置：结构体定义（OPCUAConfig / NATSConfig / CollectorConfig / AppConfig）及 Viper 加载器，支持 YAML 解析、环境变量覆盖、默认值填充 |
| `internal/model/` | 数据模型：DataPoint（数据点）、NATSMessage（发布消息）、NodeDataState（节点状态）、CollectorStats（统计信息） |
| `internal/nats/` | NATS 发布者：连接建立、单条/批量 Publish、自动重连、连接状态回调 |
| `internal/opcua/` | OPC UA 客户端：Connect（TCP+Session）、Subscribe（创建订阅与监控项）、Read 单个读取、ReadAll 批量读取、ResolveNodes 节点自动展开 |
| `pkg/pool/` | 公共对象池：泛型 Pool[T]、BytePool 字节缓冲池、SlicePool[T] 切片池（当前未被主程序使用） |

---

## 快速开始

### 环境要求

- Go 1.26+
- OPC UA Server（如 KepServer）
- NATS Server（可选，未连接时仅 WARN 日志，不会阻塞采集）

### 1. 配置文件

编辑项目根目录下的 `config.yaml`：

```yaml
app_name: go-opcua-connector
log_level: info

opcua:
  endpoint: opc.tcp://localhost:49320
  certificate: ""                   # 客户端证书路径（可选）
  private_key: ""                   # 客户端私钥路径（可选）
  username: ""
  password: ""
  security_policy: None
  security_mode: None
  connect_timeout: 30
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
  stale_threshold_sec: 10
  heartbeat_interval_sec: 5
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

#### 跨平台编译

```powershell
# Windows PowerShell → Linux (amd64)
$env:GOOS="linux"; $env:GOARCH="amd64"; go build -o go-opcua-connector-linux ./cmd/
```

```bash
# Linux → Linux (amd64)
GOOS=linux GOARCH=amd64 go build -o go-opcua-connector-linux ./cmd/

# Linux → Windows
GOOS=windows GOARCH=amd64 go build -o go-opcua-connector.exe ./cmd/
```

### 3. 运行

```bash
./go-opcua-connector
```

按 `Ctrl+C` 触发优雅关闭，程序会在 30 秒内完成正在处理的批次后退出。

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
| `security_policy` | string | — | 安全策略：`None` / `Basic128Rsa15` / `Basic256` / `Basic256Sha256` |
| `security_mode` | string | — | 安全模式：`None` / `Sign` / `SignAndEncrypt` |
| `connect_timeout` | int | `30` | 连接超时（秒） |
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
| `batch_size` | int | `100` | 每批发布的数据点数，达到即触发发布 |
| `channel_buffer_size` | int | `10000` | 内部 Channel 缓冲区大小 |
| `publish_timeout_ms` | int | `5000` | 批量发布超时，同时也是空批次刷新间隔（毫秒） |
| `subscription_topic` | string | `opcua/data` | NATS 发布主题 |
| `monitor_interval_sec` | int | `60` | 统计日志输出间隔（秒） |
| `stale_threshold_sec` | int | `10` | 数据停滞判定阈值：超过此秒数未收到更新的标记为 Stale |
| `heartbeat_interval_sec` | int | `5` | 心跳验证拉取间隔（秒） |
| `subscription_nodes` | []string | — | OPC UA 节点 ID 列表，支持文件夹级自动展开 |

---

## 环境变量覆盖

支持通过环境变量覆盖配置文件中的任意字段，将 YAML 层级以 `_` 连接并转为大写：

```powershell
# Windows PowerShell
$env:OPCUA_ENDPOINT = "opc.tcp://10.0.0.1:4840"
$env:NATS_URLS = "nats://10.0.0.2:4222"
$env:COLLECTOR_WORKER_COUNT = "20"
```

```bash
# Linux / macOS
export OPCUA_ENDPOINT="opc.tcp://10.0.0.1:4840"
export NATS_URLS="nats://10.0.0.2:4222"
export COLLECTOR_WORKER_COUNT="20"
```

---

## 节点自动展开

`subscription_nodes` 中如果配置了文件夹节点（如 `ns=2;s=t1.d1`），启动时 Collector 会自动 Browse 该节点下的所有 Variable 类型子节点，展开为叶子变量节点列表并注册到订阅中。

```
配置: ns=2;s=t1.d1（文件夹）
    ↓ Browse
ns=2;s=t1.d1.t1, ns=2;s=t1.d1.t2, ns=2;s=t1.d1.t3 ...（433 个叶子变量）
```

- 仅展开一层 Variable 子节点，不递归进入子文件夹
- 避免纳入 `_Hints` 等 KepServer 元数据节点
- Browse 失败的节点原样保留，不会丢弃
- 无子节点的配置项直接作为叶子节点订阅

---

## 架构设计

### 三线防御体系

```
第一道防线（订阅推送）
    ↓ 失败或超时
第二道防线（心跳拉取验证）
    ↓ 也失败
第三道防线（停滞检测 + Stale 标记）
    ↓
下游消费者看到 Quality="Stale" 就知道数据不再可信
```

### 数据流

```
OPC UA 订阅 → subCh → subWorker(×N) → pubCh → publishWorker → NATS.io
                  ↳ heartbeatWorker(ReadAll) → 更新 LastUpdate
                  ↳ staleCheckWorker → 检测停滞 → 产生 Stale 数据点
```

### 四条协程

| 协程 | 数量 | 触发机制 | 职责 |
|------|------|---------|------|
| `subWorker` | N（可配） | 监听 subCh 通道 | 接收订阅数据，更新节点状态，推入发布通道 |
| `heartbeatWorker` | 1 | 每 `heartbeat_interval_sec` 秒 | 调用 ReadAll 批量拉取全部节点，仅 Good 才刷新 LastUpdate |
| `staleCheckWorker` | 1 | 每 `heartbeat_interval_sec` 秒 | 扫描 LastUpdate，超过 `stale_threshold_sec` 的节点发 Stale |
| `publishWorker` | 1 | 监听 pubCh 通道 | 攒批到 `batch_size` 条或 `publish_timeout_ms` 超时，一次发 NATS |

### 背压保护

| 场景 | 机制 | 位置 |
|------|------|------|
| subCh 已满（采集器处理不过来） | `select + default` 非阻塞 drop | collector.go subWorker handler |
| pubCh 已满（NATS 发布慢） | 非阻塞 drop，Stale 数据直接丢弃 | collector.go checkStaleNodes |
| NATS 发布超时 | `context.WithTimeout(publish_timeout_ms)` | collector.go processBatch |

---

## 异常兜底设计

### 启动阶段

| 异常 | 策略 |
|------|------|
| config.yaml 不存在 | 不报错，全部使用默认值 + 环境变量 |
| config.yaml 格式错误 | 返回解析错误，程序退出 |
| OPC UA 连接失败 | Fatal，直接退出（没有 OPC UA 无法工作） |
| NATS 连接失败 | Warn，继续运行（数据不发布但采集不中断） |

### 采集阶段

| 异常 | 策略 |
|------|------|
| 节点 ID 不合法 | Warn + 跳过，不影响其他节点 |
| Browse 远程失败 | Warn + 保留原始节点直接订阅 |
| Browse 返回零子节点 | 视为叶子节点，直接订阅原 ID |
| 订阅通知 Error | Log Error + continue，不中断循环 |
| 通知 Value 为 nil | 构造 Quality="BadNoValue" 数据点，照常发出 |
| ClientHandle 越界 | NodeID 设为 "unknown"，不 panic |
| ReadAll 全部失败 | 不更新 LastUpdate，等待下一次心跳 |

### 停滞检测

| 异常 | 策略 |
|------|------|
| 订阅推送停止 + 心跳 Good | 心跳刷新 LastUpdate，不发 Stale |
| 订阅推送停止 + 心跳 Bad | LastUpdate 不变，等待过阈值后发 Stale |
| 订阅推送停止 + 心跳失败 | LastUpdate 不变，等待过阈值后发 Stale |
| 新节点初始化 | LastUpdate 设为 1 小时前，确保第一条数据到来前已被覆盖 |

### 关闭阶段

| 异常 | 策略 |
|------|------|
| Worker 卡死 | 30 秒超时后 Warn + 强制退出 |
| OPC UA 连接已断开 | `client.Connect()` 前检查 nil，Close 前检查 nil |

---

## NATS 消息格式

```json
{
  "topic": "opcua/data",
  "data_point": {
    "node_id": "ns=2;s=t1.d1.t495",
    "value": 42.5,
    "quality": "Good",
    "timestamp": "2026-05-12T10:00:00Z",
    "topic": "opcua/data"
  }
}
```

下游消费者只需判定 `data_point.quality` 字段即可获知数据状态。

---

## 程序生命周期

```
启动 → 加载配置 → 连接 OPC UA → 连接 NATS → 启动 Collector → 等待信号 → 优雅关闭
       │                      │              │
       └── 失败 → Fatal       └── 失败 → Warn └── 失败 → Fatal
                                (继续运行)      (程序终止)
```

- NATS 连接失败不阻塞启动，Collector 会以 WARN 日志运行，数据不会发布
- 收到 SIGINT/SIGTERM 后，分配 30 秒关闭窗口：cancel Context → close Channel → WaitGroup 等待 Worker 全部退出 → Close NATS → Close OPC UA

---

## 性能调优

### 10 万点位配置

```yaml
collector:
  worker_count: 10
  batch_size: 100
  channel_buffer_size: 10000
  heartbeat_interval_sec: 5
```

### 20 万点位配置

```yaml
collector:
  worker_count: 20
  batch_size: 200
  channel_buffer_size: 20000
  heartbeat_interval_sec: 10
```

### 调参指南

| 参数 | 增大效果 | 减小效果 |
|------|----------|----------|
| `worker_count` | 提升订阅数据处理并发能力 | 减少协程开销 |
| `batch_size` | 减少网络往返次数 | 降低发布延迟 |
| `channel_buffer_size` | 应对更大突发流量 | 减少内存占用 |
| `heartbeat_interval_sec` | 减少拉取开销 | 更快检测数据停滞 |
| `stale_threshold_sec` | 减少停滞误报 | 更快发现数据停滞 |

---

## 项目阅读指南

建议按依赖关系从底层到上层阅读：

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `cmd/main.go` | main() 启动 6 步流程，`initLogger()`，`loadConfig()` |
| 2 | `internal/model/datapoint.go` | DataPoint、NATSMessage、NodeDataState、CollectorStats |
| 3 | `internal/config/loader.go` → `config.go` | Viper 加载、环境变量覆盖、默认值 |
| 4 | `internal/opcua/client.go` | `Connect()` → `Subscribe()` → `handleNotifications()`（核心） → `ResolveNodes()` → `ReadAll()` |
| 5 | `internal/nats/publisher.go` | `Connect()` 三个回调 → `Publish()` → `PublishBatch()` |
| 6 | `internal/collector/collector.go` | `Start()` → `subWorker()` → `heartbeatWorker()` → `staleCheckWorker()` → `publishWorker()` |

---

## 常用命令

```powershell
# 开发调试
go run ./cmd/

# 编译检查（提交前必跑）
go build ./...

# 格式化代码（提交前必跑）
go fmt ./...

# 查看包文档
go doc ./internal/collector

# 编译 Linux 可执行文件
$env:GOOS="linux"; $env:GOARCH="amd64"; go build -o go-opcua-connector ./cmd/

# 清理依赖
go mod tidy
```

---

## 依赖

| 库 | 版本 | 用途 |
|----|------|------|
| `github.com/gopcua/opcua` | v0.6.1 | OPC UA 客户端（连接、会话、订阅、读写） |
| `github.com/nats-io/nats.go` | v1.37.0 | NATS 消息队列客户端 |
| `github.com/spf13/viper` | v1.19.0 | 配置加载（YAML + 环境变量） |
| `go.uber.org/zap` | v1.27.0 | 高性能结构化日志 |

## License

MIT