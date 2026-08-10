// Package logger 构建 zap JSON 日志（支持文件输出与轮转）。
package logger

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"influx-sync/internal/config"
)

// New 创建 zap Logger。
// - 输出：Log.File 指定时写文件（lumberjack 轮转，默认 100MB×10），否则 stderr
// - 格式：JSON
func New(cfg config.LogConfig) (*zap.Logger, error) {
	level := zapcore.InfoLevel
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = zapcore.DebugLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	case "":
		// 默认 info
	}

	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	var ws zapcore.WriteSyncer
	if cfg.File != "" {
		if err := os.MkdirAll(dirOf(cfg.File), 0o755); err != nil {
			return nil, fmt.Errorf("logger: mkdir: %w", err)
		}
		maxMB := cfg.MaxMB
		if maxMB <= 0 {
			maxMB = 100
		}
		backups := cfg.MaxBackups
		if backups <= 0 {
			backups = 10
		}
		ws = zapcore.AddSync(&lumberjack.Logger{
			Filename:   cfg.File,
			MaxSize:    maxMB,
			MaxBackups: backups,
			LocalTime:  true,
			Compress:   true,
		})
	} else {
		ws = zapcore.Lock(os.Stderr)
	}
	core := zapcore.NewCore(enc, ws, level)
	return zap.New(core, zap.AddCaller()), nil
}

func dirOf(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return "."
	}
	return p[:i]
}
