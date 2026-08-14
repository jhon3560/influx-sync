package receiver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"

	"influx-sync/internal/influx"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/wal"
)

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	t.Cleanup(func() { l.Sync() })
	return l
}

// fakeTarget 模拟目标 InfluxDB：记录写入次数与内容，可配置失败。
func fakeTarget(t *testing.T, fail bool) (*httptest.Server, *atomic.Int64, *sync.Map) {
	t.Helper()
	var writeCount atomic.Int64
	written := &sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		writeCount.Add(1)
		if fail {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "field type conflict")
			return
		}
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		written.Store("last", string(buf[:n]))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, &writeCount, written
}

func newTestReceiver(t *testing.T, srv *httptest.Server, cfg Config) *Receiver {
	t.Helper()
	c, err := influx.NewClient(influx.Config{URL: srv.URL, Database: "power", Timeout: "3s"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(c, monitor.New(), testLogger(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHandleDataFrame(t *testing.T) {
	srv, writeCount, written := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(1, []byte("m,plant=A01 value=220.5 1720000000000000000"))
	ack := r.HandleFrame(1, fb)
	if ack != protocol.AckSuccess {
		t.Fatalf("ack=%x", ack)
	}
	if writeCount.Load() != 1 {
		t.Fatalf("write count=%d", writeCount.Load())
	}
	v, _ := written.Load("last")
	if !strings.Contains(v.(string), "m,plant=A01 value=220.5") {
		t.Fatalf("written=%q", v)
	}
	if r.LastSeq() != 1 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
}

func TestHandleBadCRC(t *testing.T) {
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	fb[len(fb)-1] ^= 0xff
	ack := r.HandleFrame(1, fb)
	if ack != protocol.AckFail {
		t.Fatalf("ack=%x, want 0x00", ack)
	}
	if writeCount.Load() != 0 {
		t.Fatal("must not write on crc error")
	}
}

func TestHandleDuplicateSeq(t *testing.T) {
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("first ack=%x", ack)
	}
	// 重发同 seq
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("dup ack=%x", ack)
	}
	if writeCount.Load() != 1 {
		t.Fatalf("dup must not rewrite: writes=%d", writeCount.Load())
	}
}

func TestHandleHeartbeat(t *testing.T) {
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeHeartbeat(999)
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("heartbeat ack=%x", ack)
	}
	if writeCount.Load() != 0 {
		t.Fatal("heartbeat must not write")
	}
	if r.LastSeq() != 0 {
		t.Fatal("heartbeat must not advance last_seq")
	}
}

func TestHandleWriteFailure(t *testing.T) {
	srv, _, _ := fakeTarget(t, true)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	if ack := r.HandleFrame(1, fb); ack != protocol.AckFail {
		t.Fatalf("ack=%x, want 0x00", ack)
	}
	if r.LastSeq() != 0 {
		t.Fatal("last_seq must not advance on failure")
	}
}

func TestLastSeqPersistence(t *testing.T) {
	dir := t.TempDir()
	seqFile := filepath.Join(dir, "last_seq")
	srv, _, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{LastSeqFile: seqFile})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	r.HandleFrame(1, fb)
	if r.LastSeq() != 1 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
	// 模拟重启：新实例从文件恢复
	r2 := newTestReceiver(t, srv, Config{LastSeqFile: seqFile})
	if r2.LastSeq() != 1 {
		t.Fatalf("restored last_seq=%d", r2.LastSeq())
	}
	// 旧 seq 直接 ACK 且不写库
	fbOld, _ := protocol.EncodeData(0, []byte("m value=1 1"))
	if ack := r2.HandleFrame(1, fbOld); ack != protocol.AckSuccess {
		t.Fatalf("old seq ack=%x", ack)
	}
}

func TestSplitLines(t *testing.T) {
	lines := splitLines([]byte("a\nb\n\nc\n"))
	if len(lines) != 3 || lines[0] != "a" || lines[2] != "c" {
		t.Fatalf("lines=%v", lines)
	}
}

// --- 死信隔离（毒丸报文）测试 ---

// fakeTargetCode 返回指定 HTTP 状态码的假目标库。
func fakeTargetCode(t *testing.T, code int, body string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var writeCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		writeCount.Add(1)
		w.WriteHeader(code)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &writeCount
}

func TestPoisonPacketIsolated(t *testing.T) {
	// 毒丸：HTTP 400（字段类型冲突）→ 落盘 DLQ + 欺骗性 ACK 0xff + last_seq 推进
	dlqDir := filepath.Join(t.TempDir(), "dlq")
	srv, writeCount := fakeTargetCode(t, 400, "partial write: field type conflict")
	r := newTestReceiver(t, srv, Config{DLQDir: dlqDir})
	fb, _ := protocol.EncodeData(1, []byte("telemetry,plant=A01,point=P001 value=\"abc\" 1786234200000000000"))
	ack := r.HandleFrame(1, fb)
	if ack != protocol.AckSuccess {
		t.Fatalf("poison must get deceptive ack 0xff, got %x", ack)
	}
	if r.LastSeq() != 1 {
		t.Fatalf("last_seq=%d, want 1", r.LastSeq())
	}
	if r.metrics.PoisonCount() != 1 || r.metrics.DLQCount() != 1 {
		t.Fatalf("poison=%d dlq=%d", r.metrics.PoisonCount(), r.metrics.DLQCount())
	}
	// DLQ JSON 文件存在且字段完整
	entries, _ := os.ReadDir(dlqDir)
	if len(entries) != 1 {
		t.Fatalf("dlq files=%d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dlqDir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var meta DLQMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("dlq not valid json: %v", err)
	}
	if meta.SeqNum != 1 || meta.ErrorContext.Category != "PERMANENT_ERROR" || meta.ErrorContext.HTTPStatus != 400 {
		t.Fatalf("meta=%+v", meta)
	}
	if meta.DataMetadata.PointCount != 1 || meta.DataMetadata.Measurement != "telemetry" {
		t.Fatalf("meta data=%+v", meta.DataMetadata)
	}
	if meta.PayloadGzipBase64 == "" {
		t.Fatal("payload base64 missing")
	}
	// 重发同帧：LRU/last_seq 去重，不再写库、不重复 DLQ
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("dup ack=%x", ack)
	}
	if writeCount.Load() != 1 {
		t.Fatalf("write count=%d", writeCount.Load())
	}
	entries2, _ := os.ReadDir(dlqDir)
	if len(entries2) != 1 {
		t.Fatalf("dlq files after dup=%d", len(entries2))
	}
}

func TestTransientErrorRetries(t *testing.T) {
	// 可重试错误：HTTP 500 → 回 0x00，无 DLQ，last_seq 不推进
	dlqDir := filepath.Join(t.TempDir(), "dlq")
	srv, _ := fakeTargetCode(t, 500, "internal error")
	r := newTestReceiver(t, srv, Config{DLQDir: dlqDir})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	if ack := r.HandleFrame(1, fb); ack != protocol.AckFail {
		t.Fatalf("transient must get 0x00, got %x", ack)
	}
	if r.LastSeq() != 0 {
		t.Fatalf("last_seq must not advance, got %d", r.LastSeq())
	}
	if r.metrics.PoisonCount() != 0 {
		t.Fatalf("poison=%d", r.metrics.PoisonCount())
	}
	if entries, _ := os.ReadDir(dlqDir); len(entries) != 0 {
		t.Fatalf("dlq must be empty, got %d files", len(entries))
	}
}

// TestTransientFailureRetryNotSwallowed P0 回归：瞬时失败后重发同 seq 必须真正写库。
// 修复前：LRU 在写库前登记 seq，重试帧被 "duplicate seq (lru)" 吞掉直接回 0xff，
// 上游删除 WAL → 数据永久丢失（审计报告未覆盖，实测复现）。
func TestTransientFailureRetryNotSwallowed(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var writes atomic.Int64
	written := &sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		writes.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		written.Store("last", string(buf[:n]))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	if ack := r.HandleFrame(1, fb); ack != protocol.AckFail {
		t.Fatalf("first ack=%x, want 0x00", ack)
	}
	fail.Store(false) // 目标库恢复
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("retry ack=%x, want 0xff", ack)
	}
	if writes.Load() != 2 {
		t.Fatalf("retry must actually write (writes=%d), data would be lost otherwise", writes.Load())
	}
	v, _ := written.Load("last")
	if !strings.Contains(v.(string), "m value=1 1") {
		t.Fatalf("retry payload mismatch: %q", v)
	}
	if r.LastSeq() != 1 {
		t.Fatalf("last_seq=%d, want 1", r.LastSeq())
	}
}

// TestRelayAppendFailureGoesToDLQ C2/P0 回归：中继 WAL append 失败时 raw lines
// 落中继专用 DLQ（修复前仅记日志仍回 0xff，该帧对下一跳永久丢失）。
func TestRelayAppendFailureGoesToDLQ(t *testing.T) {
	srv, writeCount, _ := fakeTarget(t, false)
	relayDLQ := filepath.Join(t.TempDir(), "relay_dlq")
	walDir := filepath.Join(t.TempDir(), "relay_wal")
	// 段大小 1 字节：首次 append 即触发 rotate；用占位目录占住 seg-000001.log
	// 使 rotate 确定性失败（"is a directory"）
	relayWAL, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer relayWAL.Close()
	if err := os.MkdirAll(filepath.Join(walDir, "seg-000001.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := newTestReceiver(t, srv, Config{RelayWAL: relayWAL, RelayDLQDir: relayDLQ})
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	// 写目标库成功 + 中继 append 失败：仍回 0xff（主链路畅通），但数据必须进 relay DLQ
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("ack=%x, want 0xff", ack)
	}
	if writeCount.Load() != 1 {
		t.Fatalf("target writes=%d", writeCount.Load())
	}
	entries, _ := os.ReadDir(relayDLQ)
	if len(entries) != 1 {
		t.Fatalf("relay dlq files=%d, want 1 (data must not be lost)", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(relayDLQ, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var meta DLQMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("relay dlq not valid json: %v", err)
	}
	if meta.SeqNum != 1 || meta.ErrorContext.Category != "RELAY_FORWARD_FAILURE" ||
		meta.DataMetadata.SourceZone != "Zone_II" {
		t.Fatalf("meta=%+v", meta)
	}
	if meta.PayloadGzipBase64 == "" {
		t.Fatal("payload base64 missing")
	}
	if r.metrics.RelayDLQCount() != 1 {
		t.Fatalf("relay dlq metric=%d", r.metrics.RelayDLQCount())
	}
}

// TestLastPointTimestamp A5：最后一行的 ts 提取（零分配）。
func TestLastPointTimestamp(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{"m value=1 1720000000000000000\n", 1720000000000000000},
		{"m value=1 1720000000000000000", 1720000000000000000},
		{"m value=1 1\nm value=2 2\n", 2},
		{"bad", 0},
		{"", 0},
		{"m value=1 x", 0},
	}
	for _, c := range cases {
		if got := lastPointTimestamp([]byte(c.raw)); got != c.want {
			t.Fatalf("lastPointTimestamp(%q)=%d, want %d", c.raw, got, c.want)
		}
	}
}

// TestSeqTrackerOrdered 并发流水线模式下 last_seq 只推进连续前缀。
func TestSeqTrackerOrdered(t *testing.T) {
	tr := newSeqTracker()
	tr.init(0)
	if tr.done(2) {
		t.Fatal("seq 2 done before 1 must not advance")
	}
	if tr.load() != 0 {
		t.Fatalf("last=%d", tr.load())
	}
	if !tr.done(1) || tr.load() != 2 {
		t.Fatalf("after 1: last=%d, want 2", tr.load())
	}
	// 大跳跃直接越过
	if !tr.done(100_000+10) || tr.load() != 100_000+10 {
		t.Fatalf("big jump: last=%d", tr.load())
	}
}

// TestOrderedReceiverNoAdvancePastInFlight A2 数据安全：帧 k+1 先完成不得推进
// last_seq 越过在途的帧 k（否则 k 失败重传会被 seq<=last_seq 吞掉）。
func TestOrderedReceiverNoAdvancePastInFlight(t *testing.T) {
	// 按 payload 区分失败：value=1 的帧瞬时失败，其余成功
	var writes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		writes.Add(1)
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		if strings.Contains(string(buf[:n]), "value=1") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	r := newTestReceiver(t, srv, Config{})
	fb1, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	fb2, _ := protocol.EncodeData(2, []byte("m value=2 2"))
	// 并发：帧 2 先完成，帧 1 失败
	var wg sync.WaitGroup
	var ack1, ack2 byte
	wg.Add(2)
	go func() { defer wg.Done(); ack1 = r.HandleFrame(1, fb1) }()
	go func() { defer wg.Done(); ack2 = r.HandleFrame(1, fb2) }()
	wg.Wait()
	if ack1 != protocol.AckFail {
		t.Fatalf("ack1=%x, want 0x00", ack1)
	}
	if ack2 != protocol.AckSuccess {
		t.Fatalf("ack2=%x, want 0xff", ack2)
	}
	// 帧 2 完成但帧 1 在途：last_seq 不得越过 1
	if r.LastSeq() != 0 {
		t.Fatalf("last_seq=%d, must not advance past in-flight seq 1", r.LastSeq())
	}
	// 帧 1 重传成功 → last_seq 连续推进到 2
	if ack := r.HandleFrame(1, fb1); ack != protocol.AckFail {
		t.Fatalf("retry1 ack=%x (server still failing value=1)", ack)
	}
	if r.LastSeq() != 0 {
		t.Fatalf("last_seq=%d after failed retry", r.LastSeq())
	}
	// 帧 2 重传（已被 LRU 去重）：不重复写、不破坏连续推进
	if ack := r.HandleFrame(1, fb2); ack != protocol.AckSuccess {
		t.Fatalf("dup2 ack=%x", ack)
	}
	if r.LastSeq() != 0 {
		t.Fatalf("last_seq=%d, frame 1 still missing", r.LastSeq())
	}
}

func TestPoisonNoDLQDirFallsBackToNack(t *testing.T) {
	// 未配置 DLQ 目录：毒丸退回 0x00（保守：不丢数据）
	srv, _ := fakeTargetCode(t, 400, "bad request")
	r := newTestReceiver(t, srv, Config{}) // 无 DLQDir
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	if ack := r.HandleFrame(1, fb); ack != protocol.AckFail {
		t.Fatalf("must fall back to 0x00, got %x", ack)
	}
	if r.LastSeq() != 0 {
		t.Fatal("last_seq must not advance")
	}
}

func TestClassifyWriteError(t *testing.T) {
	cases := []struct {
		err       string
		permanent bool
		status    int
	}{
		{"influx: write http 400: partial write: unable to parse", true, 400},
		{"influx: write http 413: too large", true, 413},
		{"influx: write http 500: internal", false, 500},
		{"influx: write http 503: unavailable", false, 503},
		{"influx: write 5 lines: Post: dial tcp: timeout", false, 0},
	}
	for _, c := range cases {
		perm, status, _ := classifyWriteError(fmt.Errorf("%s", c.err))
		if perm != c.permanent || status != c.status {
			t.Fatalf("classify(%q) = %v,%d want %v,%d", c.err, perm, status, c.permanent, c.status)
		}
	}
}

func TestMeasurementOf(t *testing.T) {
	if m := measurementOf([]string{"telemetry,plant=A01 value=1 1"}); m != "telemetry" {
		t.Fatalf("m=%q", m)
	}
	if m := measurementOf([]string{"a b"}); m != "a" {
		t.Fatalf("m=%q", m)
	}
	if m := measurementOf(nil); m != "" {
		t.Fatalf("m=%q", m)
	}
}

func TestSeqJumpTooLargeAccepted(t *testing.T) {
	// 大序号跳跃帧（如 last_seq 丢失后 sender WAL 仍有积压）：接受处理
	// （Influx 幂等覆盖保证安全），拒绝会导致停等重发死锁
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	// 先正常处理 seq=1
	fb1, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	if ack := r.HandleFrame(1, fb1); ack != protocol.AckSuccess {
		t.Fatalf("ack=%x", ack)
	}
	// 跳跃 100 万：接受（幂等覆盖），推进 last_seq
	fbBig, _ := protocol.EncodeData(1_000_001, []byte("m value=2 2"))
	if ack := r.HandleFrame(1, fbBig); ack != protocol.AckSuccess {
		t.Fatalf("big jump must be accepted, got %x", ack)
	}
	if r.LastSeq() != 1_000_001 {
		t.Fatalf("last_seq should advance: %d", r.LastSeq())
	}
	if writeCount.Load() != 2 {
		t.Fatalf("write count=%d", writeCount.Load())
	}
}

func TestSeqSmallJumpAllowed(t *testing.T) {
	// 小跳跃（如 sender 重启后 seq 连续性轻微断档）仍**接受处理**（0xff，不拒绝）；
	// N2：last_seq 只按连续前缀推进（恒开 seqTracker），跳过的 seq 不等永不来的
	// 缺口帧——正确性由幂等重写兜底：重传帧会被再次写入（At-Least-Once）。
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(5, []byte("m value=1 1")) // last_seq=0，跳 5
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("small jump ack=%x", ack)
	}
	if r.LastSeq() != 0 {
		t.Fatalf("last_seq=%d, must not advance past permanent gap (contiguous advancement)", r.LastSeq())
	}
	if writeCount.Load() != 1 {
		t.Fatalf("write count=%d", writeCount.Load())
	}
	// 重传同 seq：不再有 LRU，走幂等重写（数据不丢，允许重复写）
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("retry ack=%x", ack)
	}
	if writeCount.Load() != 2 {
		t.Fatalf("retry must be idempotently rewritten (writes=%d), never silently swallowed", writeCount.Load())
	}
}
