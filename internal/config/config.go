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
	"influx-sync/internal/protocol"
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
		Interval          string `yaml:"interval"`            // 轮询周期
		Window            string `yaml:"window"`              // 查询窗口
		Watermark         string `yaml:"watermark"`           // 水位延迟
		MaxWindow         string `yaml:"max_window"`          // 窗口上限（防时间跳变）
		BatchPoints       int    `yaml:"batch_points"`        // 单帧点数
		WindowTarget      int    `yaml:"window_target"`       // N16 窗口增长目标点数（默认=batch_points）
		QueryLimit        int    `yaml:"query_limit"`         // 单次查询 LIMIT（分页粒度）
		ParallelQueries   int    `yaml:"poller_parallel"`     // 多窗口并行查询数（0=默认4/1=串行）
		SignalListen      string `yaml:"signal_listen"`       // 订阅信号监听地址（如 ":18098"）；空=纯轮询
		SignalMinInterval string `yaml:"signal_min_interval"` // 订阅信号最小查询间隔（默认 200ms）
		FastPath          struct {
			Listen      string `yaml:"listen"`       // A4 fast-path 订阅监听地址；空=禁用
			Mode        string `yaml:"mode"`         // off=仅信号 / auto、on=启用即透传（V1.7：auto≡on，不再等游标追平）
			DedupWindow string `yaml:"dedup_window"` // 去重集保留窗口（默认 watermark+5s）
		} `yaml:"fast_path"`
		Backfill     string   `yaml:"backfill"`     // 首次启动回填：游标初始化为 now-watermark-backfill
		TagColumns   []string `yaml:"tag_columns"`  // 显式 tag 列（空=自动 SHOW TAG KEYS 发现）
		Measurements []string `yaml:"measurements"` // 同步的 measurement 列表
	} `yaml:"sync"`
	WAL struct {
		Path        string `yaml:"path"`
		SegmentSize string `yaml:"segment_size"` // 如 64MB
	} `yaml:"wal"`
	TCP struct {
		Addr        string `yaml:"addr"`
		Timeout     string `yaml:"timeout"`
		DialTimeout string `yaml:"dial_timeout"`
		Compression string `yaml:"compression"` // 帧压缩算法：zstd（默认，V1.6）/ gzip（兼容旧接收端）
	} `yaml:"tcp"`
	Sender struct {
		MaxRetry          int    `yaml:"max_retry"`
		BackoffBase       string `yaml:"backoff_base"`
		BackoffMax        string `yaml:"backoff_max"`
		HeartbeatInterval string `yaml:"heartbeat_interval"`
		Pipeline          int    `yaml:"pipeline_window"` // A1 滑窗（实验项）：默认 1=停等；>1 需确认装置支持
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
		MaxInflight int    `yaml:"max_inflight"` // A2 并发写库流水线窗口（默认 8）
		MaxConns    int    `yaml:"max_conns"`    // 最大并发连接（0=不限制）
	} `yaml:"tcp"`
	Dedup struct {
		Cap         int    `yaml:"cap"`
		LastSeqFile string `yaml:"last_seq_file"`
	} `yaml:"dedup"`
	DLQ struct {
		Dir string `yaml:"dir"` // 毒丸死信目录（空=禁用 DLQ，退回 0x00 重试）
	} `yaml:"dlq"`
	Relay struct {
		Addr    string `yaml:"addr"`    // 中继目标地址（如 "198.51.100.x:28103"）；空=不启用中继
		WALDir  string `yaml:"wal_dir"` // 转发 WAL 目录（重启不丢，必须配置）
		Timeout string `yaml:"timeout"` // 转发读写超时
		DLQDir  string `yaml:"dlq_dir"` // 转发失败转存目录（C2）；空=默认 <wal_dir>/../relay_dlq
	} `yaml:"relay"`
	RelayWAL *wal.WAL      `yaml:"-"` // 转发 WAL 句柄（程序内注入，非配置文件）
	Monitor  MonitorConfig `yaml:"monitor"`
	Log      LogConfig     `yaml:"log"`
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

// Validate 校验必填项与取值范围（dur 解析失败/负值不再静默回退默认值）。
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
	for name, s := range map[string]string{
		"sync.interval":               c.Sync.Interval,
		"sync.window":                 c.Sync.Window,
		"sync.watermark":              c.Sync.Watermark,
		"sync.max_window":             c.Sync.MaxWindow,
		"sync.signal_min_interval":    c.Sync.SignalMinInterval,
		"sync.fast_path.dedup_window": c.Sync.FastPath.DedupWindow,
		"tcp.timeout":                 c.TCP.Timeout,
		"tcp.dial_timeout":            c.TCP.DialTimeout,
		"sender.backoff_base":         c.Sender.BackoffBase,
		"sender.backoff_max":          c.Sender.BackoffMax,
		"sender.heartbeat_interval":   c.Sender.HeartbeatInterval,
	} {
		if err := validateDur(name, s); err != nil {
			return err
		}
	}
	if c.Sender.Pipeline < 0 {
		return fmt.Errorf("config: sender.pipeline_window must be >= 0")
	}
	if c.Sync.WindowTarget < 0 {
		return fmt.Errorf("config: sync.window_target must be >= 0")
	}
	if m := c.Sync.FastPath.Mode; m != "" && m != "off" && m != "auto" && m != "on" {
		return fmt.Errorf("config: sync.fast_path.mode must be off/auto/on, got %q", m)
	}
	if cp := c.TCP.Compression; cp != "" && cp != "zstd" && cp != "gzip" {
		return fmt.Errorf("config: tcp.compression must be zstd/gzip, got %q", cp)
	}
	if b := strings.TrimSpace(c.Sync.Backfill); b != "" && b != "all" && b != "0" {
		d, err := parseDurationExt(b)
		if err != nil {
			return fmt.Errorf("config: sync.backfill must be all/0/时长(如 30d), got %q", b)
		}
		if d < 0 {
			return fmt.Errorf("config: sync.backfill: negative duration %q not allowed", b)
		}
	}
	return nil
}

// CompressionFrameType 返回数据帧类型（= 压缩算法标识）。
// 默认 zstd（TypeDataZstd，V1.6）；配置 gzip 兼容旧接收端（混合版本升级期使用）。
func (c *SenderConfig) CompressionFrameType() uint8 {
	if c.TCP.Compression == "gzip" {
		return protocol.TypeData
	}
	return protocol.TypeDataZstd
}

// validateDur 校验时长配置：空合法（用默认值），非空必须可解析且非负。
// 支持扩展单位 d（天）=24h（V1.7）。
func validateDur(name, s string) error {
	if s == "" {
		return nil
	}
	d, err := parseDurationExt(s)
	if err != nil {
		return fmt.Errorf("config: %s: bad duration %q: %w", name, s, err)
	}
	if d < 0 {
		return fmt.Errorf("config: %s: negative duration %q not allowed", name, s)
	}
	return nil
}

// Validate 校验必填项与取值范围。
func (c *ReceiverConfig) Validate() error {
	if c.Target.URL == "" || c.Target.Database == "" {
		return fmt.Errorf("config: target.url and target.database required")
	}
	if c.TCP.Listen == "" {
		return fmt.Errorf("config: tcp.listen required")
	}
	if c.TCP.MaxInflight < 0 {
		return fmt.Errorf("config: tcp.max_inflight must be >= 0")
	}
	if c.TCP.MaxConns < 0 {
		return fmt.Errorf("config: tcp.max_conns must be >= 0")
	}
	for name, s := range map[string]string{
		"tcp.read_timeout": c.TCP.ReadTimeout,
		"relay.timeout":    c.Relay.Timeout,
	} {
		if err := validateDur(name, s); err != nil {
			return err
		}
	}
	return nil
}

// --- 转换为各模块配置 ---

func dur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := parseDurationExt(s)
	if err != nil {
		return def
	}
	return d
}

// parseDurationExt 解析扩展时长：在 time.ParseDuration 基础上支持 d（天）=24h。
// 例：30d、1d12h、0.5d、12h30m。负值允许解析（由 validateDur 拒绝）。
func parseDurationExt(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	var sb strings.Builder
	i := 0
	for i < len(s) {
		j := i
		for j < len(s) && (s[j] == '-' || s[j] == '+' || s[j] == '.' || (s[j] >= '0' && s[j] <= '9')) {
			j++
		}
		if j == i {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		num := s[i:j]
		k := j
		for k < len(s) && !(s[k] == '-' || s[k] == '+' || s[k] == '.' || (s[k] >= '0' && s[k] <= '9')) {
			k++
		}
		unit := s[j:k]
		if unit == "d" {
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid day value %q in %q", num, s)
			}
			sb.WriteString(strconv.FormatFloat(f*24, 'f', -1, 64))
			sb.WriteString("h")
		} else {
			sb.WriteString(num)
			sb.WriteString(unit)
		}
		i = k
	}
	return time.ParseDuration(sb.String())
}

// PollerConfig 转换。
func (c *SenderConfig) PollerConfig() sender.PollerConfig {
	return sender.PollerConfig{
		Interval:          dur(c.Sync.Interval, time.Second),
		Window:            dur(c.Sync.Window, 5*time.Second),
		Watermark:         dur(c.Sync.Watermark, 10*time.Second),
		MaxWindow:         dur(c.Sync.MaxWindow, 30*time.Second),
		BatchPoints:       c.Sync.BatchPoints,
		WindowTarget:      c.Sync.WindowTarget,
		QueryLimit:        c.Sync.QueryLimit,
		ParallelQueries:   c.Sync.ParallelQueries,
		MinSignalInterval: dur(c.Sync.SignalMinInterval, 200*time.Millisecond),
		TagColumns:        c.Sync.TagColumns,
		Measurements:      c.Sync.Measurements,
		Compression:       c.CompressionFrameType(),
	}
}

// WatermarkDuration 返回水位延迟。
func (c *SenderConfig) WatermarkDuration() time.Duration {
	return dur(c.Sync.Watermark, 10*time.Second)
}

// FastPathConfig 转换（A4 快路径）。
func (c *SenderConfig) FastPathConfig() sender.FastPathConfig {
	watermark := c.WatermarkDuration()
	return sender.FastPathConfig{
		Mode:         sender.FastPathMode(c.Sync.FastPath.Mode),
		DedupWindow:  dur(c.Sync.FastPath.DedupWindow, watermark+5*time.Second),
		Measurements: c.Sync.Measurements,
		Compression:  c.CompressionFrameType(),
	}
}

// BackfillDuration 返回首次启动回填时长。
// BackfillMode 回填模式（V1.7）。
type BackfillMode int

const (
	BackfillNone     BackfillMode = iota // 0：仅实时
	BackfillAll                          // all：全量同步（默认）
	BackfillDuration                     // Nd：有界回填
)

// BackfillSpec 解析后的回填配置。
type BackfillSpec struct {
	Mode BackfillMode
	Dur  time.Duration // 仅 BackfillDuration 有效
}

// BackfillSpec 解析 sync.backfill：默认（空）与 "all" 均按全量处理；"0" 为仅实时；
// 其余为时长（支持 d）。V1.7.1：0 时长（如 "0d"）与负值归入仅实时——
// 负值已被 Validate 拦截，此处兜底不落入全量（防意外全库重发）。
func (c *SenderConfig) BackfillSpec() BackfillSpec {
	v := strings.TrimSpace(c.Sync.Backfill)
	if v == "" || v == "all" {
		return BackfillSpec{Mode: BackfillAll}
	}
	if v == "0" || v == "0s" {
		return BackfillSpec{Mode: BackfillNone}
	}
	d, err := parseDurationExt(v)
	if err != nil || d <= 0 {
		return BackfillSpec{Mode: BackfillNone}
	}
	return BackfillSpec{Mode: BackfillDuration, Dur: d}
}

// SenderConfig 转换（发送循环）。
func (c *SenderConfig) SenderLoopConfig() sender.SenderConfig {
	return sender.SenderConfig{
		MaxRetry:          c.Sender.MaxRetry,
		BackoffBase:       dur(c.Sender.BackoffBase, time.Second),
		BackoffMax:        dur(c.Sender.BackoffMax, 60*time.Second),
		HeartbeatInterval: dur(c.Sender.HeartbeatInterval, 30*time.Second),
		Pipeline:          c.Sender.Pipeline,
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
		MaxInflight: c.TCP.MaxInflight,
		MaxConns:    c.TCP.MaxConns,
	}
}

// ReceiverConfig 转换（Receiver 模块）。
// N2：last_seq 按序推进（seqTracker）在 Receiver 内恒开，与 TCP MaxInflight
// 的实际生效值无关——默认配置（max_inflight 空→服务端按 8 生效）不再出现
// "流水线服务端 + 非按序推进"的危险错配。
func (c *ReceiverConfig) ReceiverConfig() receiver.Config {
	return receiver.Config{
		LastSeqFile: c.Dedup.LastSeqFile,
		DLQDir:      c.DLQ.Dir,
		RelayWAL:    c.RelayWAL, // 由 main 注入（需要 wal 包），nil=不启用中继
		RelayDLQDir: c.Relay.DLQDir,
	}
}

// RelayTimeout 返回中继转发超时（C5：配置文件解析后真正生效）。
func (c *ReceiverConfig) RelayTimeout() time.Duration {
	return dur(c.Relay.Timeout, 10*time.Second)
}
