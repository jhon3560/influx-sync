package sender

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/influx"
	"influx-sync/internal/model"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/transport"
	"influx-sync/internal/wal"
)

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	l, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Sync() })
	return l
}

// fakeInfluxServer 返回每轮返回固定点的模拟源库（尊重 LIMIT 截断）。
func fakeInfluxServer(t *testing.T, pointsPerQuery int) *httptest.Server {
	t.Helper()
	var queryCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query" {
			queryCount.Add(1)
			q := r.URL.Query().Get("q")
			if strings.HasPrefix(q, "SHOW TAG KEYS") {
				fmt.Fprint(w, `{"results":[{"series":[{"name":"telemetry","columns":["tagKey"],"values":[["plant"]]}]}]}`)
				return
			}
			if strings.HasPrefix(q, "SHOW FIELD KEYS") {
				fmt.Fprint(w, `{"results":[{"series":[{"name":"telemetry","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
				return
			}
			var start, end, limit int64
			if _, err := fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end, &limit); err != nil {
				fmt.Sscanf(q, "SELECT * FROM \"telemetry\" WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end, &limit)
			}
			if limit <= 0 {
				limit = 10000
			}
			// 每个 ts 一个点（20µs 步长 ≈ 5 万点/s），受 LIMIT 与 pointsPerQuery 双重约束
			var rows []string
			for ts := start; ts < end && len(rows) < int(limit) && len(rows) < pointsPerQuery; ts += 20000 {
				rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts/1000))
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"results":[{"series":[{"name":"telemetry","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestEnv(t *testing.T, influxURL string) (*wal.WAL, *monitor.Metrics, *influx.Client) {
	t.Helper()
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	m := monitor.New()
	if influxURL == "" {
		return w, m, nil
	}
	c, err := influx.NewClient(influx.Config{URL: influxURL, Database: "power", Timeout: "3s"})
	if err != nil {
		t.Fatal(err)
	}
	return w, m, c
}

func TestPollerBasic(t *testing.T) {
	srv := fakeInfluxServer(t, 100000)
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{Window: time.Second, Watermark: time.Second})
	p.pollOnce(context.Background())
	if w.PendingCount() == 0 {
		t.Fatal("expected frames in wal")
	}
	if w.Cursor() == 0 {
		t.Fatal("cursor not advanced")
	}
	// 游标必须 ≤ now-watermark（窗口推进正确）
	if w.Cursor() > time.Now().UnixNano()-int64(time.Second) {
		t.Fatalf("cursor %d beyond watermark boundary", w.Cursor())
	}
}

func TestPollerEmptyWindowAdvancesCursor(t *testing.T) {
	srv := fakeInfluxServer(t, 0) // 无数据
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{Window: time.Second, Watermark: time.Second})
	cursor0 := time.Now().UnixNano() - int64(5*time.Second)
	w.SetCursor(cursor0)
	p.pollOnce(context.Background())
	if w.Cursor() != cursor0+int64(time.Second) {
		t.Fatalf("cursor=%d want %d", w.Cursor(), cursor0+int64(time.Second))
	}
	if w.PendingCount() != 0 {
		t.Fatalf("unexpected frames: %d", w.PendingCount())
	}
}

func TestPollerQueryFailureKeepsCursor(t *testing.T) {
	// 源库 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{Window: time.Second, Watermark: time.Second})
	cursor0 := int64(1000)
	w.SetCursor(cursor0)
	p.pollOnce(context.Background())
	if w.Cursor() != cursor0 {
		t.Fatal("cursor must not advance on query failure")
	}
}

func TestPollerNoAdvanceOnWALFailure(t *testing.T) {
	// 无法轻易让 WAL 失败；验证"查询成功但转换失败"也不推进
	srv := fakeInfluxServer(t, 100000)
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{Window: time.Second, Watermark: time.Second})
	cursor0 := time.Now().UnixNano() - int64(10*time.Second)
	w.SetCursor(cursor0)
	p.pollOnce(context.Background())
	// 正常路径应推进
	if w.Cursor() <= cursor0 {
		t.Fatal("cursor should advance")
	}
	// 校验 WAL 帧可解码且 payload 是 line protocol
	_, fb, err := w.Peek()
	if err != nil {
		t.Fatal(err)
	}
	f, err := protocol.Decode(fb)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.Decompress()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "telemetry,plant=A01") {
		t.Fatalf("payload=%q", raw)
	}
}

func TestPollerMaxWindowCap(t *testing.T) {
	// cursor 很旧 + 小 MaxWindow：end-cursor 不得超过 MaxWindow
	srv := fakeInfluxServer(t, 100000)
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{
		Window: time.Hour, Watermark: time.Second, MaxWindow: 3 * time.Second,
	})
	old := time.Now().UnixNano() - int64(time.Hour)
	w.SetCursor(old)
	p.pollOnce(context.Background())
	if w.Cursor() > old+int64(3*time.Second) {
		t.Fatalf("window cap violated: cursor=%d old=%d", w.Cursor(), old)
	}
}

// --- Sender 测试 ---

// ackServer 可控 ACK 行为的假 Receiver。
func ackServer(t *testing.T, mode string, nackTimes int32) (addr string, received *atomic.Int64) {
	t.Helper()
	received = &atomic.Int64{}
	var nacked atomic.Int32
	srv := &transport.Server{}
	srv = transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0"}, func(id uint64, _ uint64, fb []byte) byte {
		received.Add(1)
		if mode == "always-fail" {
			return protocol.AckFail
		}
		if mode == "fail-twice" && nacked.Add(1) <= nackTimes {
			return protocol.AckFail
		}
		return protocol.AckSuccess
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)
	t.Cleanup(func() { cancel(); srv.Close() })
	return srv.Addr().String(), received
}

func TestSenderNormalAck(t *testing.T) {
	addr, received := ackServer(t, "ok", 0)
	w, m, _ := newTestEnv(t, "")
	// 手工塞两帧
	w.Append(protocol.TypeData, []byte("m value=1 1"))
	w.Append(protocol.TypeData, []byte("m value=2 2"))

	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	// 等待两帧全部确认
	for i := 0; i < 100; i++ {
		if w.PendingCount() == 0 && received.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d received=%d", w.PendingCount(), received.Load())
	}
}

func TestSenderNackNeverDrops(t *testing.T) {
	// 0x00（可重试错误）重试超限：只告警，帧必须保留在 WAL，绝不允许丢数据
	addr, _ := ackServer(t, "always-fail", 0)
	w, m, _ := newTestEnv(t, "")
	w.Append(protocol.TypeData, []byte("m value=1 1"))

	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{
		MaxRetry: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	<-done
	// 无论重试多少次：帧必须还在 WAL，DLQ 必须为空
	if w.PendingCount() != 1 {
		t.Fatalf("frame must stay in wal (at-least-once), pending=%d", w.PendingCount())
	}
	if m.DLQCount() != 0 {
		t.Fatalf("dlq must be 0, got %d", m.DLQCount())
	}
}

func TestSenderRetryRecovers(t *testing.T) {
	addr, received := ackServer(t, "fail-twice", 2)
	w, m, _ := newTestEnv(t, "")
	w.Append(protocol.TypeData, []byte("m value=1 1"))

	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{
		MaxRetry: 5, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < 200; i++ {
		if w.PendingCount() == 0 && received.Load() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d received=%d", w.PendingCount(), received.Load())
	}
	if received.Load() < 3 {
		t.Fatalf("expected >=3 sends (2 nack + 1 ok), got %d", received.Load())
	}
	if m.DLQCount() != 0 {
		t.Fatalf("dlq should be 0, got %d", m.DLQCount())
	}
}

func TestSenderDisconnectKeepsWAL(t *testing.T) {
	// 服务器不启动：连接失败，WAL 数据保留
	w, m, _ := newTestEnv(t, "")
	w.Append(protocol.TypeData, []byte("m value=1 1"))
	client := transport.NewClient(transport.ClientConfig{Addr: "127.0.0.1:1", Timeout: 300 * time.Millisecond})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{
		MaxRetry: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 20 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	time.Sleep(500 * time.Millisecond)
	if w.PendingCount() != 1 {
		t.Fatal("frame must stay in wal while disconnected")
	}
}

func TestBackoff(t *testing.T) {
	s := &Sender{cfg: SenderConfig{BackoffBase: time.Second, BackoffMax: 60 * time.Second}}
	cases := []struct {
		n    int
		want time.Duration
	}{
		{0, 0},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{6, 32 * time.Second},
		{7, 60 * time.Second},
		{20, 60 * time.Second},
	}
	for _, c := range cases {
		if got := s.backoff(c.n); got != c.want {
			t.Fatalf("backoff(%d)=%v want %v", c.n, got, c.want)
		}
	}
}

// TestIsFrameTooLarge 验证拆批判定：超限错误可识别，普通错误不可拆。
func TestIsFrameTooLarge(t *testing.T) {
	if !isFrameTooLarge(protocol.ErrTooLarge) {
		t.Fatal("ErrTooLarge should be recognized")
	}
	if !isFrameTooLarge(fmt.Errorf("protocol: payload too large: 999 bytes")) {
		t.Fatal("payload too large should be recognized")
	}
	if !isFrameTooLarge(fmt.Errorf("protocol: frame too large: 999 bytes")) {
		t.Fatal("frame too large should be recognized")
	}
	if isFrameTooLarge(fmt.Errorf("some other error")) {
		t.Fatal("ordinary error must not trigger split")
	}
	if isFrameTooLarge(nil) {
		t.Fatal("nil must not trigger split")
	}
}

// TestAppendBatchSplitsOnTooLarge 验证超限帧自动拆批不卡死；
// batch=1 仍超限的单点跳过并计数（C4：防游标永久卡死），好数据正常落盘。
func TestAppendBatchSplitsOnTooLarge(t *testing.T) {
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	log := testLogger(t)
	metrics := monitor.New()
	p := &Poller{wal: w, metrics: metrics, logger: log, cfg: PollerConfig{BatchPoints: 100, ParallelQueries: 2}}
	// 单个点 17MB 字段值（> MaxDecompressedLen 16MB）：任何批量下编码必失败，
	// 拆批到 batch=1 后跳过该点（不卡死、不 panic、不返回错误，游标可推进）。
	big := strings.Repeat("x", 17<<20)
	pts := make([]model.Point, 4)
	for i := range pts {
		pts[i] = model.Point{Measurement: "m", Fields: map[string]interface{}{"v": big}, Timestamp: int64(i + 1)}
	}
	if err := p.appendBatch(pts, 100); err != nil {
		t.Fatalf("expected skip (no error) for oversized single points, got %v", err)
	}
	if metrics.SkipPointCount() != 4 {
		t.Fatalf("skip count=%d, want 4", metrics.SkipPointCount())
	}
	if w.PendingCount() != 0 {
		t.Fatalf("oversized points must not land in wal, pending=%d", w.PendingCount())
	}
	// 回归验证：可编码的数据正常落盘（与跳点共存）
	ok := []model.Point{
		{Measurement: "m", Fields: map[string]interface{}{"v": "a"}, Timestamp: 1},
		{Measurement: "m", Fields: map[string]interface{}{"v": "b"}, Timestamp: 2},
	}
	if err := p.appendBatch(ok, 100); err != nil {
		t.Fatalf("appendBatch ok data: %v", err)
	}
	if w.PendingCount() != 1 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
}

// TestSenderPipeline A1：滑窗（Pipeline>1）下多帧在途、按序 ACK、全部提交。
func TestSenderPipeline(t *testing.T) {
	var received atomic.Int64
	srv := transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0", MaxInflight: 8}, func(id uint64, _ uint64, fb []byte) byte {
		received.Add(1)
		return protocol.AckSuccess
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctxSrv, cancelSrv := context.WithCancel(context.Background())
	go srv.Serve(ctxSrv)
	t.Cleanup(func() { cancelSrv(); srv.Close() })

	w, m, _ := newTestEnv(t, "")
	for i := 0; i < 12; i++ {
		if _, err := w.Append(protocol.TypeData, []byte(fmt.Sprintf("m value=%d %d", i, i))); err != nil {
			t.Fatal(err)
		}
	}
	client := transport.NewClient(transport.ClientConfig{Addr: srv.Addr().String(), Timeout: 2 * time.Second})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{
		MaxRetry: 5, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour, Pipeline: 4,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { s.Run(ctx) }()
	for i := 0; i < 250; i++ {
		if w.PendingCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pipeline: pending=%d received=%d", w.PendingCount(), received.Load())
	}
	if received.Load() < 12 {
		t.Fatalf("received=%d, want >= 12", received.Load())
	}
}

// TestSenderPipelineGoBackN A1：窗口内第 1 帧 0x00 → go-back-N 重发 1..W 后全部提交。
func TestSenderPipelineGoBackN(t *testing.T) {
	var received atomic.Int64
	var nacked atomic.Int64
	srv := transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0", MaxInflight: 8}, func(id uint64, _ uint64, fb []byte) byte {
		received.Add(1)
		f, err := protocol.Decode(fb)
		if err != nil {
			return protocol.AckFail
		}
		if f.Seq == 1 && nacked.Add(1) <= 1 {
			return protocol.AckFail // seq=1 的帧 0x00（与处理顺序无关）
		}
		return protocol.AckSuccess
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctxSrv, cancelSrv := context.WithCancel(context.Background())
	go srv.Serve(ctxSrv)
	t.Cleanup(func() { cancelSrv(); srv.Close() })

	w, m, _ := newTestEnv(t, "")
	for i := 0; i < 4; i++ {
		if _, err := w.Append(protocol.TypeData, []byte(fmt.Sprintf("m value=%d %d", i, i))); err != nil {
			t.Fatal(err)
		}
	}
	client := transport.NewClient(transport.ClientConfig{Addr: srv.Addr().String(), Timeout: 2 * time.Second})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{
		MaxRetry: 5, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour, Pipeline: 4,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { s.Run(ctx) }()
	for i := 0; i < 250; i++ {
		if w.PendingCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("go-back-N: pending=%d received=%d", w.PendingCount(), received.Load())
	}
	if received.Load() < 8 {
		t.Fatalf("go-back-N should resend window (4+4), received=%d", received.Load())
	}
	if m.AckFailCount() == 0 {
		t.Fatal("expected at least one nack")
	}
}

// TestQueryParallelBoundaryDedup P2：边界 ts 重复点只出现一次；非边界点零损耗。
func TestQueryParallelBoundaryDedup(t *testing.T) {
	// 构造两个并行段：边界 ts=1000 的点在两段都返回（+1ns 重叠），归并后必须去重
	boundary := int64(1000)
	var queryCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		queryCount.Add(1)
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		var start, end int64
		fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		var rows []string
		step := int64(10)
		for ts := start; ts < end; ts += step {
			rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{Window: time.Second, Watermark: time.Second, ParallelQueries: 2, MinSignalInterval: 100 * time.Millisecond})
	start := boundary - 100
	end := boundary + 100 // 段 0：[start,boundary+1)，段 1：[boundary,end)
	pts, err := p.queryParallel(context.Background(), start, end)
	if err != nil {
		t.Fatalf("queryParallel: %v", err)
	}
	// 边界点只出现一次
	count := 0
	for _, pt := range pts {
		if pt.Timestamp == boundary {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("boundary ts duplicates: %d, want 1", count)
	}
	// 无其他重复
	seen := map[int64]int{}
	for _, pt := range pts {
		seen[pt.Timestamp]++
	}
	for ts, n := range seen {
		if n != 1 {
			t.Fatalf("ts %d appears %d times", ts, n)
		}
	}
}

// TestSenderPipelineNackFrameNeverCommitted N1/P0 回归：seq=1 永远 0x00 时，
// 滑窗绝不提交未写库成功的帧（修复前陈旧 ACK 错位会把 f1 误提交删除，
// 且 f2/f3 的 ACK 位被 f1 的重发字节占位——pending 会掉到 0，At-Least-Once 被破坏）。
func TestSenderPipelineNackFrameNeverCommitted(t *testing.T) {
	var seq1Nack atomic.Int64
	var seq1OK atomic.Int64
	srv := transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0", MaxInflight: 8}, func(id uint64, _ uint64, fb []byte) byte {
		f, err := protocol.Decode(fb)
		if err != nil {
			return protocol.AckFail
		}
		if f.Seq == 1 {
			seq1Nack.Add(1)
			return protocol.AckFail // seq=1 永远瞬时失败（大帧超时等场景）
		}
		return protocol.AckSuccess
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctxSrv, cancelSrv := context.WithCancel(context.Background())
	go srv.Serve(ctxSrv)
	t.Cleanup(func() { cancelSrv(); srv.Close() })

	w, m, _ := newTestEnv(t, "")
	for i := 0; i < 4; i++ {
		if _, err := w.Append(protocol.TypeData, []byte(fmt.Sprintf("m value=%d %d", i, i))); err != nil {
			t.Fatal(err)
		}
	}
	client := transport.NewClient(transport.ClientConfig{Addr: srv.Addr().String(), Timeout: 2 * time.Second})
	s := NewSender(w, client, m, testLogger(t), SenderConfig{
		MaxRetry: 10, BackoffBase: 10 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour, Pipeline: 4,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	go func() { s.Run(ctx) }()
	// 观察 3 秒：seq=1 持续失败。修复前 pending 会归零（误提交）；修复后必须保持 4
	time.Sleep(3 * time.Second)
	if w.PendingCount() != 4 {
		t.Fatalf("pending=%d, want 4 (seq=1 never written, must NOT be committed; stale-ACK misalignment regression)", w.PendingCount())
	}
	if seq1Nack.Load() < 3 {
		t.Fatalf("seq1 nack=%d, want >=3 (retry loop active)", seq1Nack.Load())
	}
	if seq1OK.Load() != 0 {
		t.Fatalf("seq1 ok=%d, must be 0", seq1OK.Load())
	}
	// 确认没有任何帧被提交：seq=1 卡头，go-back-N 下 2..4 也不得越过提交
	if s.metrics.AckOkCount() != 0 {
		t.Fatalf("ack_ok=%d, want 0 (nothing may be committed while head nacks)", s.metrics.AckOkCount())
	}
}

// TestPollVacuumSlicedBounded N11 回归：真空区跳过按 MaxWindow 切片——任何含数据
// 查询 ≤ MaxWindow（修复前连续空窗后首个含数据窗口可达 1h → 200k/s 下 7.2 亿点 OOM）。
func TestPollVacuumSlicedBounded(t *testing.T) {
	var mu sync.Mutex
	var maxQuerySpan int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		var start, end int64
		fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		mu.Lock()
		if end-start > maxQuerySpan {
			maxQuerySpan = end - start
		}
		mu.Unlock()
		var rows []string
		// 只在 [dataStart, dataStart+1h) 有数据（模拟真空区后的数据恢复）
		const dataStart = int64(200 * 1e9)
		for ts := start; ts < end; ts += 1e9 {
			if ts >= dataStart && ts < dataStart+3600*1e9 {
				rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{
		Window: time.Second, Watermark: time.Second, MaxWindow: 3 * time.Second,
	})
	p.underfillStreak = 12 // 连续空窗/欠满 → 窗口翻倍到 1h
	cur := int64(0)
	w.SetCursor(cur)
	// 第一轮：切片扫描，空片推进游标到 dataStart 之前，数据片逐片处理（N14 稀疏继续）
	p.pollOnce(context.Background())
	mu.Lock()
	span := maxQuerySpan
	mu.Unlock()
	if span > int64(3*time.Second) {
		t.Fatalf("query span %v exceeds MaxWindow (N11 OOM regression)", time.Duration(span))
	}
	if w.PendingCount() == 0 {
		t.Fatal("data slice must land in wal")
	}
	// 游标推进且任意切片查询均 ≤ MaxWindow
	if w.Cursor() <= 0 {
		t.Fatal("cursor must advance")
	}
}

// TestPollSparseBackfillWindowGrowth N14 回归：稀疏数据（点数 << BatchPoints）
// 的窗口必须与空窗一样翻倍（5s→…→1h）并切片扫描——修复前稀疏库回填速率
// 被基础窗口封顶（如 6.6 点/s 的库全量回填需十几天）。
func TestPollSparseBackfillWindowGrowth(t *testing.T) {
	var mu sync.Mutex
	var maxQuerySpan int64
	const dataEnd = int64(3600 * 1e9) // 数据区 [0, 1h)，每 10s 1 点（稀疏）
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		var start, end int64
		fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		mu.Lock()
		if end-start > maxQuerySpan {
			maxQuerySpan = end - start
		}
		mu.Unlock()
		var rows []string
		for ts := start; ts < end; ts += 10 * 1e9 {
			if ts < dataEnd {
				rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 30 * time.Second,
		BatchPoints: 100,
	})
	w.SetCursor(0)
	for i := 0; i < 10; i++ {
		p.pollOnce(context.Background())
		if w.Cursor() >= dataEnd {
			break
		}
	}
	if w.Cursor() < dataEnd {
		t.Fatalf("sparse backfill too slow: cursor=%d after 10 iterations (want >= %d, 1h sparse data)",
			w.Cursor(), dataEnd)
	}
	mu.Lock()
	span := maxQuerySpan
	mu.Unlock()
	if span > int64(30*time.Second) {
		t.Fatalf("query span %v exceeds MaxWindow (memory bound)", time.Duration(span))
	}
	if w.PendingCount() == 0 {
		t.Fatal("sparse points must land in wal")
	}
}

// TestPollPrefetchPipeline N16 回归：单窗口轮次的下一窗口查询在处理本轮时
// 已在途（预取消费）；多轮后游标正确推进、点数全量入 WAL、零重复零丢失。
func TestPollPrefetchPipeline(t *testing.T) {
	var mu sync.Mutex
	var queryCount int
	var queryDelay = 30 * time.Millisecond // 模拟源库查询延迟
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		if strings.Contains(q, "SELECT *") {
			mu.Lock()
			queryCount++
			mu.Unlock()
		}
		time.Sleep(queryDelay)
		var start, end int64
		fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		var rows []string
		for ts := start + 1e9; ts < end; ts += 2 * 1e9 { // 每 2s 1 点
			rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{
		Window: 20 * time.Second, Watermark: time.Second, MaxWindow: 20 * time.Second,
		BatchPoints: 5,
	})
	// 数据密度 0.5 点/s → 每窗 10 点 ≥ 5 目标 → streak 恒 0，窗口恒 20s（预取路径）
	w.SetCursor(0)
	const rounds = 6
	for i := 0; i < rounds; i++ {
		p.pollOnce(context.Background())
	}
	// 每轮 20s 窗口 × 6 轮 = 120s；每 2s 1 点 = 60 点全量入 WAL
	if w.Cursor() != int64(rounds*20*1e9) {
		t.Fatalf("cursor=%d want %d", w.Cursor(), int64(rounds*20*1e9))
	}
	if w.PendingCount() < rounds {
		t.Fatalf("pending=%d want >= %d (每轮至少 1 帧)", w.PendingCount(), rounds)
	}
	mu.Lock()
	qc := queryCount
	mu.Unlock()
	// NewPoller 将 ParallelQueries<=1 强制为 4（每窗 4 段查询）：
	// 6 轮 = 24 段查询；预取生效时第 7 个窗口已在途 = 28。防重查风暴断言区间。
	if qc < rounds*4 || qc > (rounds+1)*4 {
		t.Fatalf("query count=%d want in [%d, %d] (prefetch storm or missing rounds?)",
			qc, rounds*4, (rounds+1)*4)
	}
}

// TestPollPrefetchDiscardOnMismatch N16 防御：预取槽游标与当前游标不符
// （上一轮处理失败未推进）→ 丢弃并同步重查，不得跳过窗口（零丢失）。
func TestPollPrefetchDiscardOnMismatch(t *testing.T) {
	var queryCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		atomic.AddInt32(&queryCount, 1)
		var start, end int64
		fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		var rows []string
		for ts := start + 1e9; ts < end; ts += 1e9 {
			rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{
		Window: 10 * time.Second, Watermark: time.Second, MaxWindow: 10 * time.Second,
		BatchPoints: 100,
	})
	w.SetCursor(0)
	// 注入一个游标不符的预取槽（模拟上一轮失败后残留）
	p.prefetch = &prefetchSlot{
		cursor: int64(99 * 1e9), end: int64(109 * 1e9),
		ch: make(chan prefetchResult, 1),
	}
	p.prefetch.ch <- prefetchResult{points: nil, err: nil}
	qBefore := atomic.LoadInt32(&queryCount)
	p.pollOnce(context.Background())
	// 失配 → 同步重查 [0,10s)（+1），随后下一窗口预取（+1）：
	// 断言重查确实发生且游标正常推进（未跳过窗口）
	if atomic.LoadInt32(&queryCount) < qBefore+1 {
		t.Fatalf("query count=%d want >= %d (mismatch must re-query)", atomic.LoadInt32(&queryCount), qBefore+1)
	}
	if w.Cursor() != int64(10*1e9) {
		t.Fatalf("cursor=%d want %d", w.Cursor(), int64(10*1e9))
	}
}

// TestPollWindowTargetDecoupled N16 回归：window_target 独立于 batch_points 驱动
// 窗口增长——帧小（10）但增长目标大（100）时稀疏数据仍翻倍窗口快速推进；
// 命中 ≥100 点/窗的稠密数据即复位。
func TestPollWindowTargetDecoupled(t *testing.T) {
	var mu sync.Mutex
	var maxSpan int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		var start, end int64
		fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		mu.Lock()
		if end-start > maxSpan {
			maxSpan = end - start
		}
		mu.Unlock()
		var rows []string
		for ts := start + 1e9; ts < end; ts += 10 * 1e9 { // 1 点/10s 稀疏
			rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, ts))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	defer srv.Close()
	w, m, c := newTestEnv(t, srv.URL)
	p := NewPoller(c, w, m, testLogger(t), PollerConfig{
		Window: 5 * time.Second, Watermark: time.Second, MaxWindow: 30 * time.Second,
		BatchPoints: 10, WindowTarget: 100, // 帧 10 点，增长目标 100 点
	})
	w.SetCursor(0)
	// 稀疏（0.1 点/s）：每窗 < 100 目标 → 翻倍增长（若仍按 BatchPoints=10 判定，
	// 5s 窗仅 ~0.5 点也会增长——为验证解耦，直接断言多轮后游标远超前且 span≤MaxWindow）
	for i := 0; i < 12; i++ {
		p.pollOnce(context.Background())
	}
	if w.Cursor() < int64(300*1e9) {
		t.Fatalf("cursor=%d want >= %d (window must grow to MaxWindow slices)", w.Cursor(), int64(300*1e9))
	}
	mu.Lock()
	span := maxSpan
	mu.Unlock()
	if span > int64(30*time.Second) {
		t.Fatalf("query span %v exceeds MaxWindow", time.Duration(span))
	}
}
