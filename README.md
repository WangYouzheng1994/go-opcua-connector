# go-opcua-connector

OPC UA 数据采集连接器，通过 MQTT 或 NATS 发布数据，并支持消息驱动的 OPC UA 回写。

## 项目简介

从 OPC UA Server（如 KepServer）发现并订阅工业点位，通过协议无关的 StateStore 保存最新可信状态，再按即时或定时模式发布。

### 核心特性

| 特性 | 说明 |
|------|------|
| **统一状态池** | `StateStore` 以 PointID 保存最新值、健康状态、时间和联合 Generation |
| **双模式推送** | 即时模式发送合并后的最新变化状态，定时模式发送全量快照；均支持过滤后分批 |
| **可信度验证** | 主动读取验证订阅值并确认静态值，不覆盖更新的订阅结果 |
| **节点自动展开** | 支持通配符递归展开，灵活配置精确或全量采集 |
| **断线恢复** | 读取 gopcua 真实连接状态，使用双 Generation 隔离旧通知并完整重建采集 |
| **退出处理** | 捕获 SIGINT/SIGTERM，取消采集并限时等待 Collector 任务退出，再关闭共享连接 |
| **环境变量覆盖** | 支持覆盖 YAML 或加载器中已注册的配置键，详见下文 |

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
│   └── main.go                 # 应用入口，初始化与生命周期管理
├── internal/
│   ├── collector/
│   │   ├── state.go            # PointID 状态池与联合 Generation
│   │   ├── acquisition.go      # 发现、订阅、验证、重试和断线恢复
│   │   ├── source.go           # 协议无关采集接口与标准化结果
│   │   ├── publisher.go        # 发布接口
│   │   ├── health_stats.go     # 健康状态统计
│   │   └── collector.go        # 生命周期门面与双模式发送适配
│   ├── config/
│   │   ├── loader.go           # Viper 加载与环境覆盖
│   │   └── config.go           # 配置结构体定义与校验
│   ├── logging/                # 控制台/文件日志与按大小轮转
│   ├── model/
│   │   └── datapoint.go        # DataPoint、批量消息和 CollectorStats
│   ├── mqtt/
│   │   └── client.go           # MQTT 发布与回写传输
│   ├── nats/
│   │   └── client.go           # NATS 发布与回写传输
│   ├── opcua/
│   │   ├── adapter.go          # SourceAdapter 实现与连接状态适配
│   │   ├── browse.go           # 节点发现与 PointID 生成
│   │   ├── acquisition.go      # 监控项管理、订阅通知与主动读取
│   │   ├── write_target.go     # PointID 回写目标与内部 NodeID 操作
│   │   └── client.go           # 连接与身份映射
│   └── writeback/
│       ├── protocol.go         # 统一 Request / Result 协议
│       ├── value_preparer.go   # DataType Cache 与受控类型转换
│       ├── executor.go         # 固定队列与 Single Worker
│       ├── transport.go        # 回写传输接口
│       └── engine.go           # 订阅、入队、结果发布与 Shutdown
├── pkg/
│   └── pool/
│       └── pool.go             # 通用对象池工具，主链路未使用
├── config.yaml                 # 本地配置，由模板复制，不纳入版本控制
├── config.yaml.example         # 配置模板（带完整注释）
├── go.mod                      # Go 模块定义
├── go.sum                      # 依赖校验
└── README.md                   # 本文件
```

结构图省略各包的 `doc.go` 和测试文件；`.codex/` 保存本地辅助文档、归档及诊断脚本，不参与正常应用构建。

---

## 目录职责

| 目录 | 职责 |
|------|------|
| `cmd/` | 入口：初始化日志/配置、连接 OPC UA 与 MQTT/NATS、启动 Collector/Writeback、监听退出信号 |
| `internal/collector/` | 协议无关状态池、采集协调、断线恢复与即时/定时发送适配 |
| `internal/config/` | 应用、日志、OPC UA、MQTT/NATS、采集及回写配置的定义、默认值、校验和加载 |
| `internal/logging/` | zap 控制台/文件双输出，日志轮转和启动时过期清理 |
| `internal/model/` | 采集发布消息和 CollectorStats 数据模型 |
| `internal/mqtt/` | MQTT 客户端：连接、批量数据发布、回写命令订阅与结果发布 |
| `internal/nats/` | NATS 客户端：连接、Core Publish、回写命令订阅与连接状态回调 |
| `internal/opcua/` | OPC UA 客户端与 Adapter：Browse、PointID/SourceRef 映射、订阅、读取、连接状态及封装 NodeID 的 WriteTarget |
| `internal/writeback/` | PointID 回写协议、DataType Cache、受控转换、固定队列、Single Worker、聚合结果与有限优雅退出 |
| `pkg/pool/` | 通用对象池工具，当前主链路未使用；与 StateStore 状态池无关 |

---

## 快速开始

### 环境要求

- Go 1.20+
- OPC UA Server（如 KepServer）
- 与 `output_type` 对应的 MQTT Broker 或 NATS Server

### 1. 配置文件

首次使用时，在项目根目录将 [config.yaml.example](config.yaml.example) 复制为 `config.yaml`，再修改服务地址、认证信息和真实点位。已有本地配置时直接编辑，不要覆盖。

```powershell
# Windows PowerShell，仅在尚无 config.yaml 时执行
Copy-Item config.yaml.example config.yaml
```

```bash
# Linux / macOS，仅在尚无 config.yaml 时执行
cp config.yaml.example config.yaml
```

以下为 NATS 模式的简化配置；完整配置及 MQTT 字段见模板：

```yaml
app_name: go-opcua-connector
pprof_port: 6060
log:
  level: info

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
  output_type: nats
  batch_size: 100
  publish_timeout_ms: 5000
  subscription_topic: opcua/data
  monitor_interval_sec: 60
  stale_threshold_sec: 30
  heartbeat_interval_sec: 5
  subscription_retry_interval_sec: 30
  read_batch_size: 500
  read_timeout_sec: 10
  push_mode: timed               # immediate 或 timed
  push_interval_sec: 1           # 定时模式推送间隔
  force_heartbeat: false         # 即时模式下是否发送主动验证刷新
  filter_bad_quality: false
  subscription_nodes:
    - ns=2;s=Channel1.Device1.Temperature
```

程序从**当前工作目录**搜索 `config` 配置文件；相对证书路径和日志目录也以工作目录为基准，不保证相对于可执行文件。配置中的点位必须替换为实际存在的节点。

### 2. 编译

```powershell
# Windows PowerShell
go build -o go-opcua-connector.exe ./cmd/
```

```bash
# Linux / macOS
go build -o go-opcua-connector ./cmd/
```

若本地已有完整 `vendor/`，可显式添加 `-mod=vendor` 使用本地依赖。日常编译不需要先执行 `go mod tidy`。

### 3. 运行

```powershell
# Windows PowerShell，在配置文件所在目录运行
.\go-opcua-connector.exe
```

```bash
# Linux / macOS，在配置文件所在目录运行
./go-opcua-connector
```

按 `Ctrl+C` 触发退出：先停止接收新回写请求，当前回写 Request 最多获得 10 秒 grace period；剩余 Queue 直接丢弃。随后停止 Collector，最后关闭中间件与 OPC UA 连接。gopcua v0.5.3 的锁等待或 TCP Write 可能无法及时响应 context，因此 10 秒是业务取消边界，不是底层 I/O 的严格物理上限。

---

## 配置说明

下列表格中的字段相对于各小节对应的 YAML 节点；完整示例见 [config.yaml.example](config.yaml.example)。

### 应用与日志

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `app_name` | `go-opcua-connector` | 应用日志标识 |
| `pprof_port` | `6060` | pprof HTTP 端口，当前绑定所有网卡，无独立关闭开关 |
| `log.level` | `info` | 日志级别，使用此键；顶层 `log_level` 不生效 |
| `log.dir` | `./logs` | 日志目录，主日志为 `app.log` |
| `log.max_age_day` | `3` | 轮转日志保留天数；当前仅在日志初始化时清理过期文件 |
| `log.max_size_mb` | `100` | 单文件轮转大小阈值（MB） |

### OPC UA

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `endpoint` | string | — | 必填，OPC UA 服务器地址，格式 `opc.tcp://host:port` |
| `certificate` | string | `""` | 客户端 X.509 证书文件路径 |
| `private_key` | string | `""` | 客户端私钥文件路径 |
| `username` | string | `""` | 用户名（匿名认证时留空） |
| `password` | string | `""` | 密码 |
| `security_policy` | string | 空字符串；模板为 `None` | 按服务端端点设置，例如 `None` / `Basic256Sha256` |
| `security_mode` | string | 空字符串按 `None` 处理 | 支持 `None` / `Sign` / `SignAndEncrypt`；其他值也按 `None` 处理 |
| `request_timeout` | int | `30` | 请求超时（秒） |
| `point_id_overrides` | list | `[]` | 可选 PointID 覆盖，每项包含完整 NodeID `source_ref` 和显式 `point_id` |

用户名和密码都非空才启用账号认证；证书和私钥都非空才加载客户端证书。当前找不到指定安全策略/模式的端点时，会记录警告并尝试回退到 `None + None`，应以连接日志中的最终选择为准。

### NATS

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `urls` | string | — | `collector.output_type=nats` 时必填，多个地址用逗号分隔 |
| `username` | string | `""` | 用户名 |
| `password` | string | `""` | 密码 |
| `max_reconnects` | int | `-1` | 最大重连次数，`-1` 表示无限重连 |
| `reconnect_wait_ms` | int | `1000` | 重连等待间隔（毫秒） |

NATS 账号认证仅在用户名和密码都非空时启用。当前使用 Core NATS，没有 JetStream 持久化或下游业务确认。

### MQTT

仅 `collector.output_type=mqtt` 时使用。一次运行只选择 MQTT 或 NATS 中的一种。

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `brokers` | — | MQTT 模式必填，例如 `tcp://localhost:1883`，多个地址用逗号分隔 |
| `client_id` | 空 | 留空时生成客户端 ID |
| `username` / `password` | 空 | Broker 账号 |
| `topic` | `opcua/data` | 数据发布主题，优先于 `collector.subscription_topic` |
| `qos` | `0` | MQTT 协议 QoS，使用 0 / 1 / 2 |
| `retained` | `false` | 数据消息是否保留为 retained message |
| `clean_session` | `true` | 是否使用清理会话 |
| `keepalive_sec` | `60` | Keep Alive（秒） |
| `connect_timeout_sec` | `10` | 首次连接超时（秒） |
| `publish_timeout_sec` | `5` | 当前发布链路不使用，实际使用 `collector.publish_timeout_ms` |
| `auto_reconnect` | `true` | 是否自动重连 |
| `max_reconnect_delay_sec` | `60` | 重连等待上限（秒） |
| `tls_cert_file` / `tls_key_file` | 空 | TLS 客户端证书和私钥，均非空时加载 |
| `tls_ca_file` | 空 | Broker CA 证书路径 |
| `insecure_skip_verify` | `false` | 是否跳过服务端证书校验；自定义 TLS 配置在证书或 CA 路径非空时构建 |

配置校验会为未填写的 `mqtt.topic` 补默认值，因此只修改 `collector.subscription_topic` 不会改变正常启动路径下的 MQTT 数据主题。

### 采集器

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `output_type` | string | `nats` | 输出目标：`nats` 或 `mqtt`，同时决定回写使用的传输 |
| `worker_count` | int | `10` | 兼容字段，新 StateStore 采集链路不再使用 |
| `batch_size` | int | `100` | 每次发送的最大点位数；即时变化和定时快照都在过滤后按此值分批 |
| `channel_buffer_size` | int | `10000` | 兼容字段，新 StateStore 采集链路不再使用 |
| `publish_timeout_ms` | int | `5000` | 批量发布超时（毫秒） |
| `subscription_topic` | string | `opcua/data` | Collector 发布主题；MQTT 配置了 `mqtt.topic` 时优先使用 MQTT 主题 |
| `monitor_interval_sec` | int | `60` | 统计日志输出间隔（秒） |
| `stale_threshold_sec` | int | `30` | 确认时间过期阈值；基于 `LastConfirmedAt`，无确认时使用 `InitializedAt` |
| `heartbeat_interval_sec` | int | `5` | 健康检查间隔（秒） |
| `subscription_retry_interval_sec` | int | `30` | 待重试发现及订阅失败的扫描周期（秒）；失配复查后通过唤醒信号重建 |
| `read_batch_size` | int | `500` | 初读和周期验证每批读取的点位数 |
| `read_timeout_sec` | int | `10` | 每批主动读取的独立超时时间（秒） |
| `push_mode` | string | `timed` | 推送模式：`immediate`（即时）或 `timed`（定时） |
| `push_interval_sec` | int | `1` | 定时模式推送间隔（秒） |
| `force_heartbeat` | bool | `false` | 即时模式下是否发送主动验证产生的版本刷新；健康变化即使为 false 也可触发发送 |
| `filter_bad_quality` | bool | `false` | 发送前过滤非 Healthy 点；不影响状态池保存异常状态 |
| `subscription_nodes` | []string | — | OPC UA 节点 ID 列表，支持通配符递归展开 |

### 回写

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `write_subject` | string | `opcua/write` | 接收回写命令的 NATS Subject / MQTT Topic |
| `result_subject` | string | `opcua/write/result` | 发布回写结果的 NATS Subject / MQTT Topic |

没有独立回写启用开关。程序启动时会尝试订阅回写主题，失败仅告警，采集继续；不发送回写命令就不会主动写点。

---

## 环境变量覆盖

将 YAML 层级以 `_` 连接并转为大写，可覆盖 Viper 已知的配置键。使用时保留 YAML 中的对应键；未出现在 YAML 中且未由加载器注册的字段，不保证仅设置环境变量就能加载。`subscription_nodes`、`point_id_overrides` 等数组建议写在 YAML 中。

```powershell
# Windows PowerShell
$env:OPCUA_ENDPOINT = "opc.tcp://10.0.0.1:4840"
$env:NATS_URLS = "nats://10.0.0.2:4222"
$env:COLLECTOR_PUSH_MODE = "immediate"
$env:LOG_LEVEL = "debug"
```

```bash
# Linux / macOS
export OPCUA_ENDPOINT="opc.tcp://10.0.0.1:4840"
export NATS_URLS="nats://10.0.0.2:4222"
export COLLECTOR_PUSH_MODE="immediate"
export LOG_LEVEL="debug"
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

### 启动与恢复

1. 加载配置、日志和 pprof，连接 OPC UA 与所选中间件。
2. 发现点位、初始化 StateStore、创建订阅并分批初读。
3. 至少一个点同时满足“订阅成功、有有效值且 Healthy”，Collector 才能启动成功；空点表或全部失败会退出。
4. 启动周期验证、停滞检测、失败重试、连接监督、发送和统计任务，再启动回写监听。

运行中首次观察到 OPC UA 非 Connected 时，推进连接代次，保留旧值并标记 SourceDisconnected。底层 gopcua 负责自动连接恢复；协调器观察到 Connected 后，关闭旧订阅、重新发现全配置、重建订阅并验证。业务恢复失败时按 1/2/4/8/16/30 秒退避重试。

恢复成功同样只要求至少一个订阅成功点拥有有效 Healthy 值，不表示所有点都已恢复。应结合统计日志中的完整健康分布判断覆盖率。完整发现无诊断时才移除已不存在的旧点；部分发现失败时保留旧点。

### 推送模式

#### 即时模式 (`push_mode: immediate`)

- 监听 StateStore 的合并变化信号，扫描快照并选择新点、订阅更新或健康变化的点
- 先执行质量过滤，再按 `batch_size` 分批发送；同一消息可以包含多个变化点
- 同一点快速发生的多次变化可能合并，只保留最新状态，不保证发送每个中间变化
- `force_heartbeat=true` 时，主动验证产生的状态刷新也会发布
- 适用场景：下游需要实时数据

#### 定时模式 (`push_mode: timed`)

- 定时器周期性读取 StateStore 的**全量快照**，质量过滤后按 `batch_size` 分批发布
- 适用场景：下游需要稳定批量数据；数据量大需攒批

### 主要协程

| 协程 | 数量 | 触发机制 | 职责 |
|------|------|---------|------|
| 协议通知消费 | 每个有效 Subscription 一个 | OPC UA 通知 | 标准化样本并更新 StateStore |
| 周期验证 | 1 | 每 `heartbeat_interval_sec` 秒 | 分批主动读取，只验证状态和确认静态值 |
| 过期检测 | 1 | 每秒 | 根据 `LastConfirmedAt` 标记 Stale |
| 失败重试 | 1 | 重试周期和唤醒信号 | 重试发现失败和单点订阅失败 |
| 连接监督 | 1 | 每秒 | 检测真实连接状态并串行执行完整恢复 |
| 即时或定时发送 | 1 | 状态变化或发送周期 | 从 StateStore 快照生成现有 DataPoint 并发布 |
| `monitorStats` | 1 | 每 `monitor_interval_sec` 秒 | 统计信息输出 |

### 验证与停滞逻辑

订阅拥有运行期 Value 的唯一更新权。初读可播种无初值点，普通周期验证只确认当前值是否一致，不创建或替换 Value。读取前捕获 Version，更新时再次比较，避免旧读取结果覆盖期间已更新的状态；连接和监控代次共同隔离旧资源的通知。

逐点读取无效或失败会在允许的状态下标记 VerificationFailed；整个 Read 请求失败只记录诊断、跳过该批更新。读取值不一致时保留原值并标记 SubscriptionMismatch，等待 1 秒复查；仍不一致且期间没有更新时，唤醒单点重建。重建先推进监控代次，再移除旧监控项并创建新订阅。

停滞检测使用 `LastConfirmedAt`，无确认时使用 `InitializedAt`。持续读取一致的静态值不会仅因没有变化通知而被误判为 Stale。检测只更新 Initializing、Healthy、VerificationFailed、Stale，不覆盖其他更具体的故障状态。

---

## 节点自动展开

`subscription_nodes` 支持以下三种形式：

### 单点模式（Variable NodeID）

直接填写变量节点，例如 `ns=2;s=Channel1.Device1.Temperature`，只采集该变量。

### 直接子点模式（Object NodeID，无通配符）

配置文件夹节点，启动时 Browse 其 HasComponent 下的直接 Variable 子节点：

```
配置: ns=2;s=t1.d1（文件夹）
    ↓ Browse
ns=2;s=t1.d1.t1, ns=2;s=t1.d1.t2, ...（叶子变量）
```

### 通配模式（有 `.*` 后缀）

通过 HasComponent 和 Organizes 引用递归进入子 Object，收集 Variable 节点：

```
配置: ns=2;s=t1.d1.*（递归通配）
    ↓ Browse + 递归
t1.d1 下所有层级、所有文件夹的叶子变量
```

适用场景：KepServer 有 `Channel.Device.标记组.Tag` 多级结构时使用。

当前不按下划线前缀过滤变量。单个配置项浏览失败时保留诊断，不回退为采集文件夹本身；其他成功配置项可以继续。

### PointID 生成规则

上报 ID 根据真实 BrowsePath 生成：去掉 `Objects` 根段，各段以 `.` 连接；段内原有的 `%` 编码为 `%25`，`.` 编码为 `%2E`。例如 `Objects / Channel1 / Device1 / Temperature` 对应 `Channel1.Device1.Temperature`，并不是截取 NodeID 后半段。

`opcua.point_id_overrides` 可为完整 NodeID 指定显式 PointID。发现阶段会检查身份歧义和 PointID 冲突，冲突点不会覆盖已有映射。

---

## 数据质量判定

StateStore 使用结构化 `PointHealth` 保存健康状态。发送适配仅把 `Healthy` 转换为
`DataPoint.Quality="Good"`，其余健康状态都转换为非 Good；现有批量消息中的 `q`
因此只在点位为 Healthy 时等于 `true`。

异常状态保留最后有效值；尚无初值时可能发送 `v: null`。`filter_bad_quality=true` 时不发送非 Healthy 点，但仍保留其状态。有效的 `0`、`false` 和空字符串不会因值为空而被过滤。

---

## MQTT / NATS 消息格式

### 数据发布

定时模式取得全量快照，即时模式取得合并后的最新变化集合。两种模式都经过质量过滤和分批，再使用 `PublishBatch` 发送。比如最终有 250 个点，`batch_size=100` 时发送 100/100/50 三条消息，每条格式如下：

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
| `values` | array | 全量快照或即时变化集合中的一个分批，最多 `batch_size` 个点 |
| `values[].id` | string | 协议无关 PointID |
| `values[].v` | any | 点位当前值 |
| `values[].q` | bool | `PointHealth == Healthy` 时为 `true` |
| `values[].t` | int64 | 消息序列化时的本机 Unix 毫秒时间 |

当前 `t` 未使用内部的源观测时间 `ObservedAt` 或最后确认时间，不能单凭它判断值的实际年龄。跨批消息没有统一批次 ID 或整体完成标记。

### 回写命令

单点和批量统一使用 Request Envelope。下面是单点写，`items` 长度为 1：

```json
{
  "request_id": "abc123",
  "items": [
    {
      "point_id": "line1.motor1.speed_setpoint",
      "value": 1500
    }
  ]
}
```

批量写只是在同一个 `items` 中放入多项，数量必须为 1～100。`request_id`、`point_id` 和 `value` 必填，`value` 不能为 `null`。Decoder 严格拒绝未知字段；请求不接受 NodeID 或调用方指定的类型。

迁移期间临时兼容旧数组格式：

```json
[{"id":"t1.d1.t1","v":1}]
```

旧格式中的 `id` 按 PointID 解析，`v` 必填且不能为 `null`，数组数量仍限制为 1～100。Connector 会生成 `legacy-N` 形式的进程内请求 ID，并将请求转换到同一 Queue 和 Single Worker 执行链。该格式已弃用，仅用于旧调用方迁移；不支持旧 NodeID 对象协议或 `value_type`。

服务端 DataType 是类型权威来源，Connector 按 PointID 缓存成功读取的 DataType，再执行受控转换。Queue 固定容纳 500 个完整 Request，Single Worker 按 Request 入队顺序和 `items` 原始顺序执行。每个 Item 最多执行一次真实 OPC UA Write，不自动 retry，也不提供事务、rollback、持久化、重放或幂等。

批量请求示例：

```json
{
  "request_id": "batch-001",
  "items": [
    {"point_id": "line1.motor1.speed_setpoint", "value": 1500},
    {"point_id": "line1.motor1.enabled", "value": true}
  ]
}
```

回写和上报共用 Discovery 建立的 PointID 身份。NodeID、SourceRef、BrowsePath 和 NamespaceIndex 不进入回写消息，只在 `internal/opcua` 内部用于实际协议调用。

### 回写结果

```json
{
  "request_id": "abc123",
  "success": false,
  "results": [
    {
      "point_id": "line1.motor1.speed_setpoint",
      "success": true
    },
    {
      "point_id": "line1.motor1.enabled",
      "success": false,
      "code": "WRITE_FAILED",
      "message": "OPC UA server rejected write",
      "opcua_status": "BadNotWritable"
    }
  ]
}
```

每个 Request 只发布一条聚合 Result；`results` 数量和顺序与请求 `items` 一致。全部 Item 成功时顶层 `success=true`，否则为 `false`。格式非法或 Queue 已满等 Request 级拒绝使用 `results: []`。`opcua_status` 仅为可选诊断字段。

稳定错误码为：`INVALID_REQUEST`、`WRITE_QUEUE_FULL`、`POINT_NOT_FOUND`、`DATATYPE_READ_FAILED`、`VALUE_CONVERSION_FAILED`、`VALUE_OUT_OF_RANGE`、`WRITE_TIMEOUT`、`WRITE_FAILED`。

Write 成功只表示 OPC UA Server 接受 Write，不直接修改 StateStore。实际运行状态仍只通过 `Subscription → StateStore → Publish` 更新，也没有命令去重或写后读回确认。

### 投递与重连边界

- immediate 合并中间变化，发布失败只记录日志，没有持久化或重试队列；中间件重连也不会自动触发全量重发。
- timed 后续周期会再次发送当前状态，但不会补回历史变化。
- Core NATS Publish 返回和 MQTT QoS 都不等于下游业务处理完成。
- 发布超时结束调用方等待，不保证底层发送已终止。
- MQTT 自动重连回调目前只更新连接标记，没有重新订阅回写主题的应用代码；默认 `clean_session=true` 的回写恢复需真实联调验证。

---

## 异常兜底设计

### 采集阶段

| 异常 | 策略 |
|------|------|
| 节点 ID 不合法 | Warn + 跳过，不影响其他节点 |
| Browse 部分失败 | 保留 DiscoveryIssue，正常点继续；失败配置项后续重试 |
| 单点监控创建失败 | 标记 SubscribeFailed，只重建失败点 |
| 无效或无值通知 | 保留最后有效值并标记 SampleInvalid |
| 单点读取失败 | 保留最后有效值，在允许的健康状态下标记 VerificationFailed |
| 整个读取请求失败 | 记录请求诊断，不更新该批点状态，后续可由确认过期触发 Stale |
| 连接断开 | 推进 ConnectionGeneration，保留旧值并标记 SourceDisconnected |

### 停滞检测

| 场景 | 策略 |
|------|------|
| 订阅无变化 + 主动验证一致 | 更新 LastConfirmedAt，不修改 Value 或 ObservedAt |
| 超过阈值未被确认 | 对允许过期检测的健康状态标记 Stale，保留最后有效值 |
| 新节点初始化 | 标记 Initializing，等待订阅样本或初始读取播种 |

---

## 性能调优

### 调参指南

| 参数 | 作用 | 建议 |
|------|------|------|
| `worker_count` | 兼容字段 | 新采集链路不使用 |
| `channel_buffer_size` | 兼容字段 | 新采集链路不使用 |
| `heartbeat_interval_sec` | 主动验证频率 | 数据量大时结合 ReadBatchSize 和服务端能力调整 |
| `stale_threshold_sec` | 停滞判定阈值 | 结合验证周期、完整分批读取耗时与网络延迟设置 |
| `push_interval_sec` | 定时模式推送间隔 | 仅 timed 模式生效 |

---

## 项目阅读指南

建议先看入口和消息契约，再阅读协议适配、状态管理与发送/回写流程：

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `cmd/main.go` | main() 启动流程 |
| 2 | `internal/model/datapoint.go`、`internal/writeback/protocol.go` | 采集发送消息和统一回写协议 |
| 3 | `internal/config/config.go` | 配置结构体定义 |
| 4 | `internal/opcua/browse.go`、`acquisition.go`、`adapter.go` | PointID/SourceRef、订阅与读取适配 |
| 5 | `internal/collector/state.go`、`acquisition.go` | 状态所有权、Generation、验证、重试和恢复 |
| 6 | `internal/collector/collector.go` | 生产生命周期和发送适配 |
| 7 | `internal/mqtt/client.go`、`internal/nats/client.go` | 现有 PublishBatch 实现 |
| 8 | `internal/writeback/engine.go`、`executor.go`、`value_preparer.go` | 订阅入队、串行执行、类型转换、结果与 Shutdown |

---

## 常用命令

```powershell
# 开发调试
go run ./cmd/

# 编译检查（提交前必跑）
go build ./...

# 单元测试
go test ./...

# 静态检查（提交前必跑）
go vet ./...

# 格式化代码
go fmt ./...

# 跨平台编译
$previousGOOS = $env:GOOS
$previousGOARCH = $env:GOARCH
try {
  $env:GOOS = "linux"
  $env:GOARCH = "amd64"
  go build -o go-opcua-connector ./cmd/
} finally {
  $env:GOOS = $previousGOOS
  $env:GOARCH = $previousGOARCH
}
```

本地有完整 vendor 时，构建、测试和静态检查可添加 `-mod=vendor`。单测主要使用内存或模拟协议后端，不代替真实 OPC UA、MQTT/NATS 联调及长时间运行验证。

---

## 性能分析（pprof）

程序内置 pprof 端点，默认监听 `:6060`，端口由 `pprof_port` 配置。当前绑定所有网卡且没有认证或独立启停开关；端口启动失败只记录警告，采集继续。以下命令按默认端口演示。

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
web                  # 打开调用关系图（需安装 graphviz）
tree                 # 调用树视图
```

### 对比两次 heap 找增量泄漏

```bash
# 第一次抓取（运行一段时间后）
mkdir -p .codex/diagnostics
curl -o .codex/diagnostics/heap1.pb.gz http://localhost:6060/debug/pprof/heap

# 等待 10 分钟后第二次抓取
curl -o .codex/diagnostics/heap2.pb.gz http://localhost:6060/debug/pprof/heap

# 对比差异
go tool pprof -base .codex/diagnostics/heap1.pb.gz .codex/diagnostics/heap2.pb.gz
```

### 关注指标

| 指标 | 含义 |
|------|------|
| `inuse_space` | 当前实际占用的堆内存 |
| `alloc_space` | 累计分配总量（含已释放） |
| `alloc_space` ↑ 但 `inuse_space` 稳定 | 持续产生分配，但存活堆大致稳定；不能据此判断 GC 慢 |
| `inuse_space` 持续 ↑ | 需结合负载、GC 后趋势和对象保留关系排查，不能仅凭增长认定泄漏 |

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
