# go-opcua-connector

OPC UA 数据采集连接器，通过 MQTT 或 NATS 发布数据，并支持消息驱动的 OPC UA 回写。

## 项目简介

从 OPC UA Server（如 KepServer）发现并订阅工业点位，通过协议无关的 StateStore 保存最新可信状态，再按即时或定时模式发布。

### 核心特性

| 特性 | 说明 |
|------|------|
| **统一状态池** | `StateStore` 以 PointID 保存最新值、健康状态、时间和联合 Generation |
| **双模式推送** | 即时模式（实时推送变化）+ 定时模式（批量快照推送） |
| **可信度验证** | 主动读取验证订阅值并确认静态值，不覆盖更新的订阅结果 |
| **节点自动展开** | 支持通配符递归展开，灵活配置精确或全量采集 |
| **断线恢复** | 读取 gopcua 真实连接状态，使用双 Generation 隔离旧通知并完整重建采集 |
| **优雅关闭** | 捕获 SIGINT/SIGTERM，在期限内停止采集、恢复和发送协程 |
| **环境变量覆盖** | 支持通过环境变量覆盖配置文件中的任意字段 |

### 推送模式对比

| 模式 | 配置值 | 适用场景 |
|------|--------|---------|
| **即时模式** | `push_mode: immediate` | 下游需要实时数据；数据量适中 |
| **定时模式** | `push_mode: timed` | 下游需要稳定批量数据；数据量大需攒批 |

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
│   │   ├── state.go            # PointID 状态池与联合 Generation
│   │   ├── acquisition.go      # 发现、订阅、验证、重试和断线恢复
│   │   └── collector.go        # 生命周期门面与双模式发送适配
│   ├── config/
│   │   ├── doc.go              # 配置模块说明
│   │   └── config.go           # 配置结构体定义与校验
│   ├── model/
│   │   ├── doc.go              # 数据模型说明
│   │   └── datapoint.go        # DataPoint、批量消息、回写消息和 CollectorStats
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
| `cmd/` | 入口：初始化日志/配置、连接 OPC UA 与 MQTT/NATS、启动 Collector/Writeback、监听退出信号 |
| `internal/collector/` | 协议无关状态池、采集协调、断线恢复与即时/定时发送适配 |
| `internal/config/` | 配置：结构体定义（OPCUAConfig / NATSConfig / CollectorConfig / WritebackConfig）及 Viper 加载器 |
| `internal/model/` | 现有发布、回写消息和 CollectorStats 数据模型 |
| `internal/nats/` | NATS 客户端：连接建立、单条/批量 Publish、自动重连、连接状态回调 |
| `internal/opcua/` | OPC UA 客户端与 Adapter：Browse、PointID/SourceRef 映射、订阅、读取和连接状态 |
| `internal/writeback/` | 回写处理器：订阅 NATS 写命令、类型转换、写入 OPC UA、发布结果 |
| `pkg/pool/` | 公共对象池：泛型 Pool[T]、BytePool 字节缓冲池、SlicePool[T] 切片池 |

---

## 快速开始

### 环境要求

- Go 1.20+
- OPC UA Server（如 KepServer）
- 与 `output_type` 对应的 MQTT Broker 或 NATS Server

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
  # 可选；为完整 NodeID 显式指定协议无关 PointID
  point_id_overrides: []

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
  subscription_retry_interval_sec: 30
  read_batch_size: 500
  read_timeout_sec: 10
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
| `point_id_overrides` | list | `[]` | 可选 PointID 覆盖，每项包含完整 NodeID `source_ref` 和显式 `point_id` |

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
| `worker_count` | int | `10` | 兼容字段，新 StateStore 采集链路不再使用 |
| `batch_size` | int | `100` | 每次发送的最大点位数；即时变化和定时快照都在过滤后按此值分批 |
| `channel_buffer_size` | int | `10000` | 兼容字段，新 StateStore 采集链路不再使用 |
| `publish_timeout_ms` | int | `5000` | 批量发布超时（毫秒） |
| `subscription_topic` | string | `opcua/data` | Collector 发布主题；MQTT 配置了 `mqtt.topic` 时优先使用 MQTT 主题 |
| `monitor_interval_sec` | int | `60` | 统计日志输出间隔（秒） |
| `stale_threshold_sec` | int | `30` | 数据停滞判定阈值：超过此秒数未收到更新的标记为 Stale |
| `heartbeat_interval_sec` | int | `5` | 健康检查间隔（秒） |
| `subscription_retry_interval_sec` | int | `30` | 订阅失败及确认失配后的单点重建间隔（秒） |
| `read_batch_size` | int | `500` | 初读和周期验证每批读取的点位数 |
| `read_timeout_sec` | int | `10` | 每批主动读取的独立超时时间（秒） |
| `push_mode` | string | `timed` | 推送模式：`immediate`（即时）或 `timed`（定时） |
| `push_interval_sec` | int | `1` | 定时模式推送间隔（秒） |
| `force_heartbeat` | bool | `false` | 即时模式下是否发布主动验证产生的状态刷新；示例配置显式启用 |
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

### StateStore + 双模式推送架构

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│     OPC UA      │ ──→  │   StateStore    │ ──→  │  MQTT / NATS    │
│   (KepServer)   │      │ (PointID 状态池) │      │   (消息队列)    │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │ AcquisitionCoordinator│
                    │  验证、重试、断线恢复   │
                    └───────────────────────┘
```

### 数据流

```
Discover → StateStore Initialize → Subscribe → Initial Read
                                      ↓
                         订阅更新运行期 Value
                                      ↓
                                 StateStore
                         ┌────────────┴────────────┐
                         ↓                         ↓
                 immediate 变化快照          timed 全量快照
                         └────────────┬────────────┘
                                      ↓
                               MQTT / NATS Publisher
```

### 推送模式

#### 即时模式 (`push_mode: immediate`)

- 监听 StateStore 的合并变化信号，读取发生变化的 PointID 最新快照并逐点发布
- `force_heartbeat=true` 时，主动验证产生的状态刷新也会发布
- 适用场景：下游需要实时数据

#### 定时模式 (`push_mode: timed`)

- 定时器周期性读取 StateStore 的**全量快照**并批量发布
- 适用场景：下游需要稳定批量数据；数据量大需攒批

### 主要协程

| 协程 | 数量 | 触发机制 | 职责 |
|------|------|---------|------|
| 周期验证 | 1 | 每 `heartbeat_interval_sec` 秒 | 分批主动读取，只验证状态和确认静态值 |
| 过期检测 | 1 | 每秒 | 根据 `LastConfirmedAt` 标记 Stale |
| 失败重试 | 1 | 重试周期和唤醒信号 | 重试发现失败和单点订阅失败 |
| 连接监督 | 1 | 每秒 | 检测真实连接状态并串行执行完整恢复 |
| 即时或定时发送 | 1 | 状态变化或发送周期 | 从 StateStore 快照生成现有 DataPoint 并发布 |
| `monitorStats` | 1 | 每 `monitor_interval_sec` 秒 | 统计信息输出 |

### 验证与停滞逻辑

订阅拥有运行期 Value 的唯一更新权。主动读取的结果只能播种无初值点、确认当前值一致，
或把不一致和读取失败反映为健康状态；Version CAS 防止旧读取覆盖更新的订阅结果。
停滞检测使用 `LastConfirmedAt`，因此持续读取一致的静态值不会仅因没有变化通知而被误判为 Stale。

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

StateStore 使用结构化 `PointHealth` 保存健康状态。发送适配仅把 `Healthy` 转换为
`DataPoint.Quality="Good"`，其余健康状态都转换为非 Good；现有批量消息中的 `q`
因此只在点位为 Healthy 时等于 `true`。

---

## MQTT / NATS 消息格式

### 数据发布

定时模式（`push_mode: timed`）每个周期推送全量快照，即时模式（`push_mode: immediate`）每次变化推单条，两种模式都使用 `PublishBatch` 统一通道，格式为批量包装：

```json
{
  "timestamp": 1789372800000,
  "values": [
    {
      "id": "Channel1.Device1.Temperature",
      "v": 42.5,
      "q": true,
      "t": 1789372800000
    }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | int64 | 消息序列化时的本机 Unix 毫秒时间 |
| `values` | array | 定时模式包含过滤后的全量快照，即时模式通常包含一个变化点 |
| `values[].id` | string | 协议无关 PointID |
| `values[].v` | any | 点位当前值 |
| `values[].q` | bool | `PointHealth == Healthy` 时为 `true` |
| `values[].t` | int64 | 消息序列化时的本机 Unix 毫秒时间 |

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
| Browse 部分失败 | 保留 DiscoveryIssue，正常点继续；失败配置项后续重试 |
| 单点监控创建失败 | 标记 SubscribeFailed，只重建失败点 |
| 无效或无值通知 | 保留最后有效值并标记 SampleInvalid |
| 主动读取失败 | 保留最后有效值并记录验证异常，不覆盖订阅值 |
| 连接断开 | 推进 ConnectionGeneration，保留旧值并标记 SourceDisconnected |

### 停滞检测

| 场景 | 策略 |
|------|------|
| 订阅无变化 + 主动验证一致 | 更新 LastConfirmedAt，不修改 Value 或 ObservedAt |
| 超过阈值未被确认 | 标记 Stale，保留最后有效值 |
| 新节点初始化 | 标记 Initializing，等待订阅样本或初始读取播种 |

---

## 性能调优

### 调参指南

| 参数 | 作用 | 建议 |
|------|------|------|
| `worker_count` | 兼容字段 | 新采集链路不使用 |
| `channel_buffer_size` | 兼容字段 | 新采集链路不使用 |
| `heartbeat_interval_sec` | 主动验证频率 | 数据量大时结合 ReadBatchSize 和服务端能力调整 |
| `stale_threshold_sec` | 停滞判定阈值 | 应大于 heartbeat_interval_sec |
| `push_interval_sec` | 定时模式推送间隔 | 仅 timed 模式生效 |

---

## 项目阅读指南

建议按依赖关系从底层到上层阅读：

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `cmd/main.go` | main() 启动流程 |
| 2 | `internal/model/datapoint.go` | 现有发送和回写消息契约 |
| 3 | `internal/config/config.go` | 配置结构体定义 |
| 4 | `internal/opcua/browse.go`、`acquisition.go`、`adapter.go` | PointID/SourceRef、订阅与读取适配 |
| 5 | `internal/collector/state.go`、`acquisition.go` | 状态所有权、Generation、验证、重试和恢复 |
| 6 | `internal/collector/collector.go` | 生产生命周期和发送适配 |
| 7 | `internal/mqtt/client.go`、`internal/nats/client.go` | 现有 PublishBatch 实现 |
| 8 | `internal/writeback/engine.go` | 订阅命令、类型转换和 OPC UA 写入 |

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
| `github.com/gopcua/opcua` | v0.5.3 | OPC UA 客户端 |
| `github.com/eclipse/paho.mqtt.golang` | v1.4.3 | MQTT 客户端 |
| `github.com/nats-io/nats.go` | v1.31.0 | NATS 消息队列客户端 |
| `github.com/spf13/viper` | v1.17.0 | 配置加载（YAML + 环境变量） |
| `go.uber.org/zap` | v1.26.0 | 结构化日志 |

## License

MIT
