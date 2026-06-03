// Package logging 提供基于 zap 的生产级日志初始化。
// 支持控制台和文件双输出，文件按大小轮转、按天数自动清理。
package logging

import (
	"os"
	"path/filepath"

	"go-opcua-connector/internal/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger 根据日志配置创建 zap.Logger。
// 控制台输出始终保留，文件输出使用 rotatingWriter 管理轮转和清理。
func NewLogger(cfg *config.LogConfig) (*zap.Logger, error) {
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(cfg.Dir, "app.log")
	rw, err := newRotatingWriter(logPath, cfg.MaxSizeMB, cfg.MaxAgeDay)
	if err != nil {
		return nil, err
	}

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

	consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)
	fileEncoder := zapcore.NewJSONEncoder(encoderConfig)

	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level)
	fileCore := zapcore.NewCore(fileEncoder, zapcore.AddSync(rw), level)

	core := zapcore.NewTee(consoleCore, fileCore)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logger, nil
}
