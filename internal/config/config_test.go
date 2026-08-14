package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const senderYAML = `
source:
  url: http://127.0.0.1:8086
  database: power
sync:
  interval: 2s
  window: 6s
  watermark: 12s
  max_window: 30s
  batch_points: 5000
  measurements:
    - telemetry
wal:
  path: /data/wal
  segment_size: 64MB
tcp:
  addr: 192.168.1.10:7777
  timeout: 15s
sender:
  max_retry: 8
monitor:
  addr: :8080
log:
  level: info
`

func TestLoadSender(t *testing.T) {
	cfg, err := LoadSender(writeTemp(t, senderYAML))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.URL != "http://127.0.0.1:8086" || cfg.Source.Database != "power" {
		t.Fatalf("source=%+v", cfg.Source)
	}
	if cfg.Sync.Interval != "2s" || cfg.Sync.BatchPoints != 5000 {
		t.Fatalf("sync=%+v", cfg.Sync)
	}
	if len(cfg.Sync.Measurements) != 1 || cfg.Sync.Measurements[0] != "telemetry" {
		t.Fatalf("measurements=%v", cfg.Sync.Measurements)
	}
	p := cfg.PollerConfig()
	if p.Interval != 2*time.Second || p.Window != 6*time.Second || p.Watermark != 12*time.Second {
		t.Fatalf("poller=%+v", p)
	}
	if cfg.SegmentSize() != 64*1024*1024 {
		t.Fatalf("segment=%d", cfg.SegmentSize())
	}
	cc := cfg.ClientConfig()
	if cc.Addr != "192.168.1.10:7777" || cc.Timeout != 15*time.Second {
		t.Fatalf("client=%+v", cc)
	}
	sl := cfg.SenderLoopConfig()
	if sl.MaxRetry != 8 || sl.BackoffBase != time.Second {
		t.Fatalf("sender loop=%+v", sl)
	}
}

func TestLoadSenderDefaults(t *testing.T) {
	cfg, err := LoadSender(writeTemp(t, "source:\n  url: http://x:8086\n  database: d\ntcp:\n  addr: 1.2.3.4:5\nwal:\n  path: /tmp/w\n"))
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.PollerConfig()
	if p.Interval != time.Second || p.Window != 5*time.Second || p.Watermark != 10*time.Second {
		t.Fatalf("defaults wrong: %+v", p)
	}
	if cfg.SegmentSize() != 64<<20 {
		t.Fatalf("segment default=%d", cfg.SegmentSize())
	}
}

func TestLoadSenderValidation(t *testing.T) {
	if _, err := LoadSender(writeTemp(t, "source:\n  url: http://x\n")); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := LoadSender("/nonexistent.yaml"); err == nil {
		t.Fatal("expected read error")
	}
	if _, err := LoadSender(writeTemp(t, "not: [valid")); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLoadReceiver(t *testing.T) {
	cfg, err := LoadReceiver(writeTemp(t, `
target:
  url: http://127.0.0.1:8086
  database: power
tcp:
  listen: :7777
  read_timeout: 90s
dedup:
  cap: 20000
  last_seq_file: /data/last_seq
monitor:
  addr: :8081
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TCP.Listen != ":7777" {
		t.Fatalf("listen=%q", cfg.TCP.Listen)
	}
	sc := cfg.ServerConfig()
	if sc.ReadTimeout != 90*time.Second {
		t.Fatalf("read timeout=%v", sc.ReadTimeout)
	}
	rc := cfg.ReceiverConfig()
	if rc.LastSeqFile != "/data/last_seq" {
		t.Fatalf("receiver cfg=%+v", rc)
	}
}

func TestEnvOverride(t *testing.T) {
	os.Setenv("INFLUXSYNC_TCP_ADDR", "9.9.9.9:1")
	os.Setenv("INFLUXSYNC_SOURCE_DATABASE", "envdb")
	os.Setenv("INFLUXSYNC_LOG_LEVEL", "debug")
	defer os.Unsetenv("INFLUXSYNC_TCP_ADDR")
	defer os.Unsetenv("INFLUXSYNC_SOURCE_DATABASE")
	defer os.Unsetenv("INFLUXSYNC_LOG_LEVEL")

	cfg, err := LoadSender(writeTemp(t, senderYAML))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TCP.Addr != "9.9.9.9:1" {
		t.Fatalf("addr=%q", cfg.TCP.Addr)
	}
	if cfg.Source.Database != "envdb" {
		t.Fatalf("db=%q", cfg.Source.Database)
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("log=%q", cfg.Log.Level)
	}
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"64MB", 64 << 20, false},
		{"1GB", 1 << 30, false},
		{"512KB", 512 * 1024, false},
		{"100", 100, false},
		{"1TB", 0, true},
		{"", 0, true},
	}
	for _, c := range cases {
		got, err := parseSize(c.in)
		if c.err {
			if err == nil {
				t.Fatalf("parseSize(%q): expected error", c.in)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Fatalf("parseSize(%q)=%d,%v want %d", c.in, got, err, c.want)
		}
	}
}

// TestValidateRejectsBadDurations 小项加固：dur 解析失败/负值必须报错，不再静默回退。
func TestValidateRejectsBadDurations(t *testing.T) {
	sc := &SenderConfig{}
	sc.Source.URL = "http://x"
	sc.Source.Database = "d"
	sc.TCP.Addr = "1.1.1.1:1"
	sc.WAL.Path = "/tmp/w"
	sc.Sync.Interval = "bogus"
	if err := sc.Validate(); err == nil || !strings.Contains(err.Error(), "sync.interval") {
		t.Fatalf("bogus interval must be rejected, got %v", err)
	}
	sc.Sync.Interval = "-5s"
	if err := sc.Validate(); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("negative interval must be rejected, got %v", err)
	}
	sc.Sync.Interval = ""
	if err := sc.Validate(); err != nil {
		t.Fatalf("empty duration (use default) must pass: %v", err)
	}

	rc := &ReceiverConfig{}
	rc.Target.URL = "http://x"
	rc.Target.Database = "d"
	rc.TCP.Listen = ":1"
	rc.Relay.Timeout = "nonsense"
	if err := rc.Validate(); err == nil || !strings.Contains(err.Error(), "relay.timeout") {
		t.Fatalf("bad relay.timeout must be rejected, got %v", err)
	}
	rc.Relay.Timeout = "3s"
	if err := rc.Validate(); err != nil {
		t.Fatalf("valid relay.timeout must pass: %v", err)
	}
	// C5：RelayTimeout 转换真实生效
	if got := rc.RelayTimeout(); got != 3*time.Second {
		t.Fatalf("RelayTimeout=%v, want 3s", got)
	}
}

// TestMinSignalIntervalWired 小项修复：signal_min_interval 从 YAML 传入 PollerConfig。
func TestMinSignalIntervalWired(t *testing.T) {
	sc := &SenderConfig{}
	sc.Sync.SignalMinInterval = "50ms"
	pc := sc.PollerConfig()
	if pc.MinSignalInterval != 50*time.Millisecond {
		t.Fatalf("MinSignalInterval=%v, want 50ms", pc.MinSignalInterval)
	}
	if got := (&SenderConfig{}).PollerConfig().MinSignalInterval; got != 200*time.Millisecond {
		t.Fatalf("default MinSignalInterval=%v, want 200ms", got)
	}
}
