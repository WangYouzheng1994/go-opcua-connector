// Command go-opcua-connector connects to OPC UA servers, collects data points, and forwards them to NATS.io.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/config"
	"go-opcua-connector/internal/nats"
	"go-opcua-connector/internal/opcua"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/**
 * 主程序入口
 */
func main() {
	logger := initLogger()
	defer logger.Sync()

	logger.Info("Starting go-opcua-connector...")

	cfg, err := loadConfig()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opcuaClient := opcua.NewClient(&cfg.OPCUA, logger)

	if err := opcuaClient.Connect(ctx); err != nil {
		logger.Fatal("Failed to connect to OPC UA", zap.Error(err))
	}
	defer opcuaClient.Close()

	publisher := nats.NewPublisher(&cfg.NATS, logger)
	if err := publisher.Connect(ctx); err != nil {
		logger.Warn("Failed to connect to NATS, continuing without NATS", zap.Error(err))
	} else {
		defer publisher.Close()
	}

	col := collector.New(&cfg.Collector, opcuaClient, publisher, logger)
	if err := col.Start(); err != nil {
		logger.Fatal("Failed to start collector", zap.Error(err))
	}
	defer col.Stop()

	logger.Info("go-opcua-connector started successfully",
		zap.String("app_name", cfg.AppName),
		zap.Int("workers", cfg.Collector.WorkerCount),
		zap.Int("batch_size", cfg.Collector.BatchSize))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	logger.Info("Received shutdown signal")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	select {
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout, forcing exit")
	}
}

func initLogger() *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:      false,
		Encoding:         "console",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}

	return logger
}

func loadConfig() (*config.AppConfig, error) {
	loader := config.NewLoader(".", "config")
	cfg, err := loader.Load()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
