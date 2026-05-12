/*
Command go-opcua-connector 是 OPC UA 到 NATS.io 数据转发服务的入口程序。

启动架构：
  1. 从 config.yaml 加载配置
  2. 连接 OPC UA 服务器（KepServer）
  3. 连接 NATS.io 消息中间件
  4. 启动采集器引擎：
     - 解析配置中的文件夹节点，通过 Browse 展开为叶子变量
     - 订阅所有解析后的节点，接收实时数据变化
     - 定期执行心跳拉取，检测数据停滞
     - 批量发布数据点到 NATS

使用方式：
  go run ./cmd/            开发调试
  go build -o xxx ./cmd/   编译为可执行文件

可执行文件从同级目录读取 config.yaml。
*/
package main