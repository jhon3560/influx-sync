package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"

	"influx-sync/internal/config"
)

func TestNewStderr(t *testing.T) {
	l, err := New(config.LogConfig{Level: "info"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Sync()
	l.Info("hello")
}

func TestNewFileRotation(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "logs", "test.log")
	l, err := New(config.LogConfig{Level: "debug", File: logFile, MaxMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		l.Info("line", zap.Int("idx", i))
	}
	l.Sync()
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	// JSON 格式 + 含字段
	if !strings.Contains(string(data), `"idx"`) || !strings.Contains(string(data), `"msg":"line"`) {
		t.Fatalf("unexpected log format: %s", string(data[:200]))
	}
}

func TestNewBadLevel(t *testing.T) {
	// 未知 level 回退到 info，不报错
	if _, err := New(config.LogConfig{Level: "verbose"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDirOf(t *testing.T) {
	if dirOf("/a/b/c.log") != "/a/b" {
		t.Fatal("dirOf failed")
	}
	if dirOf("c.log") != "." {
		t.Fatal("dirOf failed")
	}
}
