# go-opcua-connector

高性能OPC UA到NATS.io数据采集连接器，支持单机10万点位采集能力。

## 项目简介

本项目用于从KepServer OPC UA Server采集数据，并推送到NATS.io消息队列。

### 核心技术特性

- **订阅+拉取复合模式**：实时订阅数据变化 + 定期拉取心跳验证
- **Worker Pool并发模型**：分散处理压力
- **批量发布**：减少网络往返开销
- **有缓冲Channel**：削峰填谷
- **NATS JetStream**：支持断点续传
- **对象池复用**：减少GC压力

### 数据质量判定

通过订阅（实时）和拉取（验证）双重机制，统一判定数据Quality：

| Quality | 含义 |
|---------|------|
| `Good` | 数据正常，订阅和拉取值一致 |
| `Stale` | 数据停滞，超过阈值未更新 |
| `Uncertain` | 数据可疑，订阅值与拉取值不一致 |
| `Bad` | 连接断开或无法读取 |

## 性能目标

| 点位规模 | 实现方案 |
|----------|----------|
| 10万点位 | 默认配置即可达到 |
| 20万点位 | 增加worker数量 + 调整batch_size |

## 项目结构

```
go-opcua-connector/
├── cmd/
│   └── main.go              # 应用程序入口
├── internal/
│   ├── collector/           # 数据采集器核心
│   │   └── collector.go     # 订阅+拉取复合模式实现
│   ├── config/              # 配置管理
│   │   ├── config.go       # 配置结构定义
│   │   └── loader.go       # Viper配置加载器
│   ├── model/               # 数据模型
│   │   └── datapoint.go    # DataPoint、NATSMessage等
│   ├── nats/                # NATS发布者
│   │   └── publisher.go   # 连接、发布、JetStream支持
│   └── opcua/               # OPC UA客户端
│       └── client.go      # 连接、订阅、拉取、回调
├── pkg/
│   └── pool/                # 对象池
│       └── pool.go        # 字节缓冲池、切片池
├── config.yaml              # 应用配置
└── go.mod                   # Go模块定义
```

## 目录职责

| 目录/文件 | 职责说明 |
|-----------|----------|
| cmd/main.go | 程序入口：初始化日志、加载配置、连接服务、启动采集器、优雅关闭 |
| internal/collector | 数据采集核心：订阅处理、拉取心跳验证、Quality判定、批量发布 |
| internal/config | 配置管理：OPC UA、NATS、采集器配置结构定义和加载 |
| internal/model | 数据模型：DataPoint数据点、NATSMessage消息、CollectorStats统计 |
| internal/nats | NATS发布：连接管理、消息发布、JetStream持久化支持 |
| internal/opcua | OPC UA客户端：服务器连接、订阅管理、主动拉取、数据回调 |
| pkg/pool | 对象池：字节缓冲区池、切片池，减少GC提升性能 |

## 快速开始

### 1. 修改配置文件

编辑 `config.yaml`：

```yaml
app_name: go-opcua-connector
log_level: info

opcua:
  endpoint: opc.tcp://你的kepserver地址:4840
  username: ""
  password: ""
  security_policy: None
  security_mode: None
  connect_timeout: 30
  request_timeout: 30

nats:
  urls: nats://localhost:4222
  max_reconnects: -1

collector:
  worker_count: 10           # Worker数量，根据CPU调整
  batch_size: 100            # 批量发布大小
  channel_buffer_size: 10000 # 通道缓冲
  publish_timeout_ms: 5000
  subscription_topic: opcua/data
  monitor_interval_sec: 60
  stale_threshold_sec: 10    # 数据停滞判定阈值（秒）
  heartbeat_interval_sec: 5  # 心跳验证间隔（秒）
  subscription_nodes:       # OPC UA节点列表
    - ns=2;i=1
    - ns=2;i=2
```

### 2. 编译项目

```bash
go mod tidy
go build -o go-opcua-connector ./cmd
```

### 3. 运行

```bash
./go-opcua-connector
```

## 配置说明

### OPC UA配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| endpoint | OPC UA服务器地址 | opc.tcp://localhost:4840 |
| username | 用户名（可选） | admin |
| password | 密码（可选） | password |
| security_policy | 安全策略 | None, Basic128Rsa15, Basic256, Basic256Sha256 |
| security_mode | 安全模式 | None, Sign, SignAndEncrypt |

### NATS配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| urls | NATS服务器地址 | nats://localhost:4222 |
| username | 用户名（可选） | - |
| password | 密码（可选） | - |
| max_reconnects | 最大重连次数 | -1表示无限 |

### 采集器配置

| 配置项 | 说明 | 推荐值 |
|--------|------|--------|
| worker_count | Worker协程数量 | CPU核数 |
| batch_size | 批量发布大小 | 100-500 |
| channel_buffer_size | 数据通道缓冲 | 10000 |
| publish_timeout_ms | 发布超时 | 5000 |
| stale_threshold_sec | 数据停滞判定阈值（秒） | 10 |
| heartbeat_interval_sec | 心跳验证间隔（秒） | 5 |

## 订阅+拉取复合模式

### 工作原理

```
┌─────────────────────────────────────────────────────────────────┐
│                          Collector                               │
│                                                                  │
│  ┌──────────────┐         ┌──────────────┐         ┌──────────┐ │
│  │  订阅数据     │         │  心跳拉取     │         │ Quality  │ │
│  │  (实时)      │         │  (定期验证)   │         │ 判定     │ │
│  └──────┬───────┘         └──────┬───────┘         └────┬─────┘ │
│         │                        │                      │        │
│         └────────────────────────┴──────────────────────┘        │
│                                  │                                │
└──────────────────────────────────┼────────────────────────────────┘
                                   ▼
                            ┌─────────────┐
                            │  NATS.io   │
                            │  (统一topic)│
                            └─────────────┘
```

### 数据流

1. **订阅通道**：实时接收OPC UA服务器推送的数据变化
2. **拉取通道**：定期主动读取节点值作为心跳验证
3. **Quality判定**：结合订阅数据和拉取结果，统一判定数据质量
4. **统一发布**：所有数据通过单一topic发布到NATS

### Quality判定规则

| 场景 | Quality | 说明 |
|------|---------|------|
| 订阅值正常更新 | `Good` | 正常数据 |
| 超过阈值未更新 | `Stale` | 数据停滞 |
| 订阅值≠拉取值 | `Uncertain` | 数据可疑 |
| 拉取失败 | `Bad` | 无法验证 |

### NATS消息格式

```json
{
  "topic": "opcua/data",
  "data_point": {
    "node_id": "ns=2;i=1",
    "value": 123.45,
    "quality": "Good",
    "timestamp": "2026-05-06T10:00:00Z"
  }
}
```

下游消费者只需判断 `quality` 字段即可知晓数据状态。

## 性能调优建议

### 10万点位配置

```yaml
collector:
  worker_count: 10
  batch_size: 100
  channel_buffer_size: 10000
  heartbeat_interval_sec: 5
```

### 20万点位配置

```yaml
collector:
  worker_count: 20
  batch_size: 200
  channel_buffer_size: 20000
  heartbeat_interval_sec: 10
```

### 关键调优点

1. **worker_count**：增加可提升并发处理能力
2. **batch_size**：增大可减少网络往返，但增加延迟
3. **channel_buffer_size**：增大可应对突发流量，但增加内存使用
4. **heartbeat_interval_sec**：增大可减少拉取开销，但检测延迟增加

## 依赖库

| 库 | 版本 | 用途 |
|----|------|------|
| github.com/gopcua/opcua | v0.6.1 | OPC UA客户端 |
| github.com/nats-io/nats.go | v1.37.0 | NATS客户端 |
| github.com/spf13/viper | v1.19.0 | 配置管理 |
| go.uber.org/zap | v1.27.0 | 日志 |

## License

MIT
