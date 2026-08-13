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
	fb, _ := protocol.EncodeData(100, []byte("m,plant=A01 value=220.5 1720000000000000000"))
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
	if r.LastSeq() != 100 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
}

func TestHandleBadCRC(t *testing.T) {
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(101, []byte("m value=1 1"))
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
	fb, _ := protocol.EncodeData(102, []byte("m value=1 1"))
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
	fb, _ := protocol.EncodeData(103, []byte("m value=1 1"))
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
	fb, _ := protocol.EncodeData(200, []byte("m value=1 1"))
	r.HandleFrame(1, fb)
	if r.LastSeq() != 200 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
	// 模拟重启：新实例从文件恢复
	r2 := newTestReceiver(t, srv, Config{LastSeqFile: seqFile})
	if r2.LastSeq() != 200 {
		t.Fatalf("restored last_seq=%d", r2.LastSeq())
	}
	// 旧 seq 直接 ACK 且不写库
	fbOld, _ := protocol.EncodeData(199, []byte("m value=1 1"))
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

// --- LRU 测试 ---

func TestLRUBasic(t *testing.T) {
	l := NewLRU(3)
	if l.CheckAndAdd(1) {
		t.Fatal("1 should be new")
	}
	if !l.CheckAndAdd(1) {
		t.Fatal("1 should be duplicate")
	}
	l.CheckAndAdd(2)
	l.CheckAndAdd(3)
	l.CheckAndAdd(4) // 淘汰 1
	if l.Contains(1) {
		t.Fatal("1 should be evicted")
	}
	if l.Len() != 3 {
		t.Fatalf("len=%d", l.Len())
	}
}

func TestLRUMoveToFront(t *testing.T) {
	l := NewLRU(2)
	l.CheckAndAdd(1)
	l.CheckAndAdd(2)
	l.CheckAndAdd(1) // 1 移到最前
	l.CheckAndAdd(3) // 淘汰 2
	if l.Contains(2) {
		t.Fatal("2 should be evicted")
	}
	if !l.Contains(1) {
		t.Fatal("1 should remain")
	}
}

func TestLRUConcurrent(t *testing.T) {
	l := NewLRU(1000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				l.CheckAndAdd(uint64(g*1000 + i))
			}
		}(g)
	}
	wg.Wait()
	if l.Len() > 1000 {
		t.Fatalf("len=%d exceeds cap", l.Len())
	}
}

func TestLRUCapacity(t *testing.T) {
	l := NewLRU(0) // 默认 10000
	for i := 0; i < 50000; i++ {
		l.CheckAndAdd(uint64(i))
	}
	if l.Len() != 10000 {
		t.Fatalf("len=%d", l.Len())
	}
	if l.Contains(0) {
		t.Fatal("oldest should be evicted")
	}
	if !l.Contains(49999) {
		t.Fatal("newest should exist")
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
	fb, _ := protocol.EncodeData(300, []byte("telemetry,plant=A01,point=P001 value=\"abc\" 1786234200000000000"))
	ack := r.HandleFrame(1, fb)
	if ack != protocol.AckSuccess {
		t.Fatalf("poison must get deceptive ack 0xff, got %x", ack)
	}
	if r.LastSeq() != 300 {
		t.Fatalf("last_seq=%d, want 300", r.LastSeq())
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
	if meta.SeqNum != 300 || meta.ErrorContext.Category != "PERMANENT_ERROR" || meta.ErrorContext.HTTPStatus != 400 {
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
	fb, _ := protocol.EncodeData(301, []byte("m value=1 1"))
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

func TestPoisonNoDLQDirFallsBackToNack(t *testing.T) {
	// 未配置 DLQ 目录：毒丸退回 0x00（保守：不丢数据）
	srv, _ := fakeTargetCode(t, 400, "bad request")
	r := newTestReceiver(t, srv, Config{}) // 无 DLQDir
	fb, _ := protocol.EncodeData(302, []byte("m value=1 1"))
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
	// 小跳跃（如 sender 重启后 seq 连续性轻微断档）仍处理
	srv, writeCount, _ := fakeTarget(t, false)
	r := newTestReceiver(t, srv, Config{})
	fb, _ := protocol.EncodeData(5, []byte("m value=1 1")) // last_seq=0，跳 5
	if ack := r.HandleFrame(1, fb); ack != protocol.AckSuccess {
		t.Fatalf("small jump ack=%x", ack)
	}
	if r.LastSeq() != 5 {
		t.Fatalf("last_seq=%d", r.LastSeq())
	}
	if writeCount.Load() != 1 {
		t.Fatalf("write count=%d", writeCount.Load())
	}
}
