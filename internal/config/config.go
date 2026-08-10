// Package config 加载 YAML 配置并支持环境变量覆盖（INFLUXSYNC_ 前缀）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"influx-sync/internal/influx"
	"influx-sync/internal/monitor"
	"influx-sync/internal/receiver"
	"influx-sync/internal/sender"
	"influx-sync/internal/transport"
	"influx-sync/internal/wal"
)

// LogConfig 日志配置。
type LogConfig struct {
	Level      string `yaml:"level"` // debug/info/warn/error
	File       string `yaml:"file"`  // 日志文件（空=stderr）
	MaxMB      int    `yaml:"max_mb"`
	MaxBackups int    `yaml:"max_backups"`
}

// MonitorConfig 指标服务。
type MonitorConfig struct {
	Addr     string `yaml:"addr"`     // 如 :8080
	Username string `yaml:"username"` // 监控端口 Basic Auth 用户名（空=不启用认证）
	Password string `yaml:"password"`
}

// Auth 返回监控端口认证配置（nil=不启用）。
func (c *MonitorConfig) Auth() *monitor.Auth {
	if c.Username == "" {
		return nil
	}
	return &monitor.Auth{Username: c.Username, Password: c.Password}
}

// SenderConfig Sender 完整配置。
type SenderConfig struct {
	Source influx.Config `yaml:"source"`
	Sync   struct {
		Interval        string   `yaml:"interval"`        // 轮询周期
		Window          string   `yaml:"window"`          // 查询窗口
		Watermark       string   `yaml:"watermark"`       // 水位延迟
		MaxWindow       string   `yaml:"max_window"`      // 窗口上限（防时间跳变）
		BatchPoints     int      `yaml:"batch_points"`    // 单帧点数
		QueryLimit      int      `yaml:"query_limit"`     // 单次查询 LIMIT（分页粒度）
		ParallelQueries int      `yaml:"poller_parallel"` // 多窗口并行查询数（0=串行，默认4）
		Backfill        string   `yaml:"backfill"`        // 首次启动回填：游标初始化为 now-watermark-backfill
		TagColumns      []string `yaml:"tag_columns"`     // 显式 tag 列（空=自动 SHOW TAG KEYS 发现）
		Measurements    []string `yaml:"measurements"`    // 同步的 measurement 列表
	} `yaml:"sync"`
	WAL struct {
		Path        string `yaml:"path"`
		SegmentSize string `yaml:"segment_size"` // 如 64MB
	} `yaml:"wal"`
	TCP struct {
		Addr        string `yaml:"addr"`
		Timeout     string `yaml:"timeout"`
		DialTimeout string `yaml:"dial_timeout"`
	} `yaml:"tcp"`
	Sender struct {
		MaxRetry          int    `yaml:"max_retry"`
		BackoffBase       string `yaml:"backoff_base"`
		BackoffMax        string `yaml:"backoff_max"`
		HeartbeatInterval string `yaml:"heartbeat_interval"`
	} `yaml:"sender"`
	Monitor MonitorConfig `yaml:"monitor"`
	Log     LogConfig     `yaml:"log"`
}

// ReceiverConfig Receiver 完整配置。
type ReceiverConfig struct {
	Target influx.Config `yaml:"target"`
	TCP    struct {
		Listen      string `yaml:"listen"`
		ReadTimeout string `yaml:"read_timeout"`
	} `yaml:"tcp"`
	Dedup struct {
		Cap         int    `yaml:"cap"`
		LastSeqFile string `yaml:"last_seq_file"`
	} `yaml:"dedup"`
	DLQ struct {
		Dir string `yaml:"dir"` // 毒丸死信目录（空=禁用 DLQ，退回 0x00 重试）
	} `yaml:"dlq"`
	Monitor MonitorConfig `yaml:"monitor"`
	Log     LogConfig     `yaml:"log"`
}

// LoadSender 加载 Sender 配置并应用环境变量覆盖。
func LoadSender(path string) (*SenderConfig, error) {
	cfg := &SenderConfig{}
	if err := loadFile(path, cfg); err != nil {
		return nil, err
	}
	applySenderEnv(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadReceiver 加载 Receiver 配置并应用环境变量覆盖。
func LoadReceiver(path string) (*ReceiverConfig, error) {
	cfg := &ReceiverConfig{}
	if err := loadFile(path, cfg); err != nil {
		return nil, err
	}
	applyReceiverEnv(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadFile(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("config: parse %s: %w", path, err)
	}
	return nil
}

// applySenderEnv 环境变量覆盖（INFLUXSYNC_ 前缀）。
func applySenderEnv(c *SenderConfig) {
	if v := os.Getenv("INFLUXSYNC_SOURCE_URL"); v != "" {
		c.Source.URL = v
	}
	if v := os.Getenv("INFLUXSYNC_SOURCE_DATABASE"); v != "" {
		c.Source.Database = v
	}
	if v := os.Getenv("INFLUXSYNC_TCP_ADDR"); v != "" {
		c.TCP.Addr = v
	}
	if v := os.Getenv("INFLUXSYNC_WAL_PATH"); v != "" {
		c.WAL.Path = v
	}
	if v := os.Getenv("INFLUXSYNC_MONITOR_ADDR"); v != "" {
		c.Monitor.Addr = v
	}
	if v := os.Getenv("INFLUXSYNC_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
}

// applyReceiverEnv 环境变量覆盖。
func applyReceiverEnv(c *ReceiverConfig) {
	if v := os.Getenv("INFLUXSYNC_TARGET_URL"); v != "" {
		c.Target.URL = v
	}
	if v := os.Getenv("INFLUXSYNC_TARGET_DATABASE"); v != "" {
		c.Target.Database = v
	}
	if v := os.Getenv("INFLUXSYNC_TCP_LISTEN"); v != "" {
		c.TCP.Listen = v
	}
	if v := os.Getenv("INFLUXSYNC_MONITOR_ADDR"); v != "" {
		c.Monitor.Addr = v
	}
	if v := os.Getenv("INFLUXSYNC_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
}

// Validate 校验必填项。
func (c *SenderConfig) Validate() error {
	if c.Source.URL == "" || c.Source.Database == "" {
		return fmt.Errorf("config: source.url and source.database required")
	}
	if c.TCP.Addr == "" {
		return fmt.Errorf("config: tcp.addr required")
	}
	if c.WAL.Path == "" {
		return fmt.Errorf("config: wal.path required")
	}
	return nil
}

// Validate 校验必填项。
func (c *ReceiverConfig) Validate() error {
	if c.Target.URL == "" || c.Target.Database == "" {
		return fmt.Errorf("config: target.url and target.database required")
	}
	if c.TCP.Listen == "" {
		return fmt.Errorf("config: tcp.listen required")
	}
	return nil
}

// --- 转换为各模块配置 ---

func dur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

// PollerConfig 转换。
func (c *SenderConfig) PollerConfig() sender.PollerConfig {
	return sender.PollerConfig{
		Interval:        dur(c.Sync.Interval, time.Second),
		Window:          dur(c.Sync.Window, 5*time.Second),
		Watermark:       dur(c.Sync.Watermark, 10*time.Second),
		MaxWindow:       dur(c.Sync.MaxWindow, 30*time.Second),
		BatchPoints:     c.Sync.BatchPoints,
		QueryLimit:      c.Sync.QueryLimit,
		ParallelQueries: c.Sync.ParallelQueries,
		TagColumns:      c.Sync.TagColumns,
		Measurements:    c.Sync.Measurements,
	}
}

// WatermarkDuration 返回水位延迟。
func (c *SenderConfig) WatermarkDuration() time.Duration {
	return dur(c.Sync.Watermark, 10*time.Second)
}

// BackfillDuration 返回首次启动回填时长。
func (c *SenderConfig) BackfillDuration() time.Duration {
	return dur(c.Sync.Backfill, 0)
}

// SenderConfig 转换（发送循环）。
func (c *SenderConfig) SenderLoopConfig() sender.SenderConfig {
	return sender.SenderConfig{
		MaxRetry:          c.Sender.MaxRetry,
		BackoffBase:       dur(c.Sender.BackoffBase, time.Second),
		BackoffMax:        dur(c.Sender.BackoffMax, 60*time.Second),
		HeartbeatInterval: dur(c.Sender.HeartbeatInterval, 30*time.Second),
	}
}

// ClientConfig 转换（TCP 客户端）。
func (c *SenderConfig) ClientConfig() transport.ClientConfig {
	return transport.ClientConfig{
		Addr:        c.TCP.Addr,
		Timeout:     dur(c.TCP.Timeout, 10*time.Second),
		DialTimeout: dur(c.TCP.DialTimeout, 10*time.Second),
	}
}

// SegmentSize 返回 WAL 段大小字节。
func (c *SenderConfig) SegmentSize() int64 {
	if c.WAL.SegmentSize == "" {
		return wal.DefaultSegmentSize
	}
	d, err := parseSize(c.WAL.SegmentSize)
	if err != nil {
		return wal.DefaultSegmentSize
	}
	return d
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, err
	}
	switch strings.ToUpper(strings.TrimSpace(s[i:])) {
	case "", "B":
		return n, nil
	case "KB":
		return n * 1024, nil
	case "MB":
		return n * 1024 * 1024, nil
	case "GB":
		return n * 1024 * 1024 * 1024, nil
	}
	return 0, fmt.Errorf("unknown unit %q", s[i:])
}

// ServerConfig 转换（TCP 服务器）。
func (c *ReceiverConfig) ServerConfig() transport.ServerConfig {
	return transport.ServerConfig{
		Listen:      c.TCP.Listen,
		ReadTimeout: dur(c.TCP.ReadTimeout, 60*time.Second),
	}
}

// ReceiverConfig 转换（Receiver 模块）。
func (c *ReceiverConfig) ReceiverConfig() receiver.Config {
	return receiver.Config{
		LastSeqFile: c.Dedup.LastSeqFile,
		DedupCap:    c.Dedup.Cap,
		DLQDir:      c.DLQ.Dir,
	}
}
