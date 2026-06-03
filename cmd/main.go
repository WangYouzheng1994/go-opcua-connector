// Command go-opcua-connector connects to OPC UA servers, collects data points, and forwards them to message brokers.
package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/logging"
	"go-opcua-connector/internal/mqtt"
	"go-opcua-connector/internal/nats"
	"go-opcua-connector/internal/opcua"
	"go-opcua-connector/internal/writeback"

	"go.uber.org/zap"
)

// main 主程序入口
func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := logging.NewLogger(&cfg.Log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting go-opcua-connector...")

	go func() {
		pprofAddr := ":6060"
		logger.Info("pprof endpoint listening", zap.String("addr", pprofAddr))
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			logger.Warn("pprof server stopped", zap.Error(err))
		}
	}()

	// 初始化通知器
	ctx, cancel := context.WithCancel(context.Background())
	// 会触发ctx.Done()信号，通知持有ctx的释放资源
	defer cancel()

	// 创建新的OPC UA客户端
	opcuaClient := opcua.NewClient(&cfg.OPCUA, logger)

	// OPC UA 连接
	if err := opcuaClient.Connect(ctx); err != nil {
		logger.Fatal("Failed to connect to OPC UA", zap.Error(err))
	}
	defer opcuaClient.Close()

	var (
		publisher   collector.Publisher
		wbTransport writeback.Transport
	)

	switch cfg.Collector.OutputType {
	case config.OutputTypeNATS:
		natsClient := nats.NewClient(&cfg.NATS, logger)
		if err := natsClient.Connect(ctx); err != nil {
			logger.Fatal("Failed to connect to NATS", zap.Error(err))
		}
		defer natsClient.Close()
		publisher = natsClient
		wbTransport = natsClient

	case config.OutputTypeMQTT:
		mqttClient := mqtt.NewClient(&cfg.MQTT, logger)
		if err := mqttClient.Connect(ctx); err != nil {
			logger.Fatal("Failed to connect to MQTT", zap.Error(err))
		}
		defer mqttClient.Close()
		publisher = mqttClient
		wbTransport = mqttClient
	}

	col := collector.New(&cfg.Collector, opcuaClient, publisher, logger)
	if err := col.Start(); err != nil {
		logger.Fatal("Failed to start collector", zap.Error(err))
	}
	defer col.Stop()

	if wbTransport != nil {
		nodeIDPrefix := extractNodeIDPrefix(cfg.Collector.SubscriptionNodes)
		wbEngine := writeback.NewEngine(opcuaClient, wbTransport, &cfg.Writeback, nodeIDPrefix, logger)
		if err := wbEngine.Start(ctx); err != nil {
			logger.Warn("Failed to start writeback engine, writeback disabled", zap.Error(err))
		} else {
			defer wbEngine.Stop()
		}
	}

	logger.Info("go-opcua-connector started successfully",
		zap.String("app_name", cfg.AppName),
		zap.String("output_type", string(cfg.Collector.OutputType)),
		zap.Int("workers", cfg.Collector.WorkerCount),
		zap.Int("batch_size", cfg.Collector.BatchSize))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号（阻塞）
	<-sigCh
	logger.Info("Received shutdown signal, stopping...")
}

func extractNodeIDPrefix(nodes []string) string {
	for _, node := range nodes {
		idx := strings.Index(node, ";s=")
		if idx >= 0 {
			return node[:idx+3]
		}
	}
	return ""
}

func loadConfig() (*config.AppConfig, error) {
	loader := config.NewLoader(".", "config")
	cfg, err := loader.Load()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
