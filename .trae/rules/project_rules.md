# go-opcua-connector 项目工程规范

## 技术栈

| 组件 | 库 | 用途 |
|------|-----|------|
| 语言 | Go 1.21+ | |
| OPC UA 客户端 | github.com/gopcua/opcua | 连接 KepServer，订阅+拉取数据 |
| 消息队列 | github.com/nats-io/nats.go | 数据发布到 NATS.io |
| 日志 | go.uber.org/zap | 结构化日志 |
| 配置 | github.com/spf13/viper | YAML 加载 + 环境变量覆盖 |
| 对象池 | 自研 pkg/pool | 泛型对象池 |

## 项目目录结构

```
cmd/main.go              程序入口
internal/
  collector/              采集引擎：订阅OPC UA数据、批量发布到NATS
  config/                 配置结构体、Viper 加载器
  model/                  数据模型（DataPoint/WriteCommand/WriteResult）
  nats/                   NATS 客户端：连接、发布、订阅、重连
  opcua/                  OPC UA 客户端：连接、订阅、读、写、节点展开
  writeback/              回写引擎：订阅NATS命令、类型转换、写入OPC UA
pkg/
  pool/                   泛型对象池（Pool[T]、BytePool、SlicePool[T]）
config.yaml               运行时配置文件
config.yaml.example       带完整注释的配置模板
```

## 配置约定

- 运行时配置文件：`config.yaml`（与可执行文件同级目录）
- 所有配置项可通过环境变量覆盖（层级以 `_` 连接并大写，如 `OPCUA_ENDPOINT`）
- Windows PowerShell：`$env:OPCUA_ENDPOINT = "opc.tcp://..."`
- 数值型配置缺失时由 `Validate()` 方法填入默认值（见 `internal/config/config.go`）

## Go 语言规范

### 注释规范

- 包注释：`// Package 包名 功能描述`，可放在独立 `doc.go` 文件
- main 包：`// Command 命令名 功能描述`
- 导出类型、函数、方法必须有注释，紧邻声明之前，之间不空行
- 结构体每个导出字段单独注释，放在字段声明上方一行
- 禁止行尾注释、禁止 `/** */` 块注释、禁止 `@param` `@return` 等 Javadoc 标签
- 注释必须是完整句子（中文以句号结尾，英文以 `.` 结尾）

### 命名规范

- 包名：全小写，无下划线
- 导出标识符：CamelCase，首字母大写
- 非导出标识符：camelCase，首字母小写
- 常量：camelCase，不使用全大写和下划线

### 代码组织

- 每文件只属于一个包，文件名全小写下划线分隔
- import 分组：标准库 → 第三方库 → 本项目包，组间空行分隔
- gofmt/goimports 自动格式化，不手动对齐

## 编译与检查

```bash
go build ./...    # 编译检查
go vet ./...      # 静态分析
```

## 规则自维护

- 本文件是项目的唯一工程规范源，不在其他位置创建分散的规范文件。
- 当编译命令、技术栈或配置方式变化时，主动更新对应章节。
- 新增约束追加到对应章节末尾，保持已有内容稳定。
