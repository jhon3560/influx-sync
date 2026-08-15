// Package integration 端到端集成测试：完整同步链路 + 断连恢复。
package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/influx"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/receiver"
	"influx-sync/internal/sender"
	"influx-sync/internal/transport"
	"influx-sync/internal/wal"
)

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	t.Cleanup(func() { l.Sync() })
	return l
}

// fakeSource 模拟源 InfluxDB：按窗口返回递增数据。
func fakeSource(t *testing.T) *httptest.Server {
	t.Helper()
	var seq atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query" {
			q := r.URL.Query().Get("q")
			var start, end int64
			if _, err := fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end); err != nil {
				fmt.Sscanf(q, "SELECT * FROM \"telemetry\" WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
			}
			var rows []string
			for ts := start; ts < end; ts += 100_000_000 { // 100ms 一个点
				rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, seq.Add(1)))
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

// fakeTarget 模拟目标 InfluxDB：统计写入行数与字节。
func fakeTarget(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var lines atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		body := string(buf[:n])
		lines.Add(int64(strings.Count(body, "\n") + 1))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, &lines
}

// startReceiverSrv 启动 Receiver TCP 服务，返回可关闭/重启的句柄。
type receiverSrv struct {
	handler *receiver.Receiver
	srv     *transport.Server
	ctx     context.Context
	cancel  context.CancelFunc
}

func startReceiverSrv(t *testing.T, targetURL string, lastSeqFile string) (*receiverSrv, string) {
	t.Helper()
	ic, err := influx.NewClient(influx.Config{URL: targetURL, Database: "power", Timeout: "3s"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := receiver.New(ic, monitor.New(), testLogger(t), receiver.Config{LastSeqFile: lastSeqFile})
	if err != nil {
		t.Fatal(err)
	}
	srv := transport.NewServer(transport.ServerConfig{Listen: "127.0.0.1:0"}, func(id uint64, fidx uint64, fb []byte) byte {
		return h.HandleFrame(id, fidx, fb)
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)
	t.Cleanup(func() { cancel(); srv.Close() })
	return &receiverSrv{handler: h, srv: srv, ctx: ctx, cancel: cancel}, srv.Addr().String()
}

func (r *receiverSrv) stop() { r.cancel(); r.srv.Close() }

func TestEndToEndSync(t *testing.T) {
	src := fakeSource(t)
	tgt, targetLines := fakeTarget(t)
	rs, addr := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq"))

	// Sender 侧
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ic, err := influx.NewClient(influx.Config{URL: src.URL, Database: "power", Timeout: "3s"})
	if err != nil {
		t.Fatal(err)
	}
	metrics := monitor.New()
	poller := sender.NewPoller(ic, w, metrics, testLogger(t), sender.PollerConfig{
		Interval: 100 * time.Millisecond, Window: 500 * time.Millisecond, Watermark: 500 * time.Millisecond,
		BatchPoints: 100,
	})
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	sl := sender.NewSender(w, client, metrics, testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: 30 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go poller.Run(ctx)
	go sl.Run(ctx)

	// 阶段1：正常运行 2.5s，目标库应持续收到数据
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if targetLines.Load() > 30 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if targetLines.Load() < 30 {
		t.Fatalf("target received too few lines: %d", targetLines.Load())
	}
	if w.PendingCount() != 0 {
		t.Fatalf("wal pending=%d after normal run", w.PendingCount())
	}
	if w.Cursor() == 0 {
		t.Fatal("cursor not advanced")
	}

	// 阶段2：断连 1.5s，WAL 应积压（Poller 继续拉取）
	rs.stop()
	time.Sleep(1500 * time.Millisecond)
	if w.PendingCount() == 0 {
		t.Fatal("expected wal backlog while disconnected")
	}
	before := targetLines.Load()

	// 阶段3：恢复连接，数据应自动补传、WAL 清空
	rs2, addr2 := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq2"))
	defer rs2.stop()
	_ = addr2
	// 新 server 端口变了，但 sender client 连接的是旧地址：改为让 sender 连接新地址
	// 简单方案：直接重建 sender client（模拟隔离恢复后地址不变）
	_ = addr // addr 是旧端口

	// 等待 WAL 清空（sender 持续重连旧地址会失败；这里用新地址验证恢复能力）
	client2 := transport.NewClient(transport.ClientConfig{Addr: addr2, Timeout: 2 * time.Second})
	sl2 := sender.NewSender(w, client2, metrics, testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: 30 * time.Second,
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go sl2.Run(ctx2)

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if w.PendingCount() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("wal pending=%d after reconnect (received %d lines)", w.PendingCount(), targetLines.Load())
	}
	if targetLines.Load() <= before {
		t.Fatal("no data delivered after reconnect")
	}
}

func TestEndToEndRestartRecovery(t *testing.T) {
	// Sender 重启后：从 WAL 恢复，重复帧由 Receiver 去重（不重复写）
	tgt, targetLines := fakeTarget(t)
	lastSeqFile := filepath.Join(t.TempDir(), "last_seq")

	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 预置一帧未确认
	seq1, err := w.Append(0x01, []byte("telemetry,plant=A01 value=1 1"))
	if err != nil {
		t.Fatal(err)
	}
	w.SetCursor(100)
	w.Close() // 模拟崩溃

	// Receiver 先处理该帧
	rs, addr := startReceiverSrv(t, tgt.URL, lastSeqFile)
	defer rs.stop()
	fb, err := encodeData(seq1, []byte("telemetry,plant=A01 value=1 1"))
	if err != nil {
		t.Fatal(err)
	}
	if ack := rs.handler.HandleFrame(1, 0, fb); ack != 0xff {
		t.Fatalf("ack=%x", ack)
	}
	if targetLines.Load() != 1 {
		t.Fatalf("lines=%d", targetLines.Load())
	}

	// Sender 重启：重发同一帧，Receiver 去重不重复写
	w2, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.PendingCount() != 1 {
		t.Fatalf("pending=%d", w2.PendingCount())
	}
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	sl := sender.NewSender(w2, client, monitor.New(), testLogger(t), sender.SenderConfig{
		MaxRetry: 3, BackoffBase: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sl.Run(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if w2.PendingCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if w2.PendingCount() != 0 {
		t.Fatal("frame not committed after resend")
	}
	// Receiver 收到重复帧：直接 ACK，不重复写
	if targetLines.Load() != 1 {
		t.Fatalf("lines=%d, want 1 (dup must be deduped)", targetLines.Load())
	}
}

func encodeData(seq uint64, payload []byte) ([]byte, error) {
	return protocol.EncodeData(seq, payload)
}

// TestEndToEndFastPath A4 e2e：快路径透传 + Poller 去重抑制二次转发（零丢失零重复）。
//
// 关键断言：推送的每个点在目标库**恰好出现一次**——快路径转发的副本 + 轮询
// 窗口的孪生副本被去重集抑制。源假库的查询网格锚定在 epoch 的 100ms 整数倍上，
// 推送 ts 取同网格点，保证轮询窗口必然返回孪生行（去重路径确定性触发）。
func TestEndToEndFastPath(t *testing.T) {
	const step = int64(100_000_000) // 100ms 网格
	// 源假库：网格锚定 epoch 整数倍（区别于共享 fakeSource 的 cursor 锚定）
	var seq atomic.Int64
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		// schema 元查询：plant=tag、value=float——保证轮询路径与快路径构造相同的 series 键
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"telemetry","columns":["tagKey"],"values":[["plant"]]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"telemetry","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		var start, end int64
		if _, err := fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end); err != nil {
			fmt.Sscanf(q, "SELECT * FROM \"telemetry\" WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		}
		var rows []string
		first := (start + step - 1) / step * step
		for ts := first; ts < end; ts += step {
			rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, seq.Add(1)))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"telemetry","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	t.Cleanup(src.Close)

	// 目标假库：统计每个 ts 出现的次数（按行尾时间戳）
	var mu sync.Mutex
	tsCount := map[int64]int{}
	var totalLines atomic.Int64
	tgt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		buf := make([]byte, 1<<20)
		n, _ := r.Body.Read(buf)
		body := string(buf[:n])
		mu.Lock()
		for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
			if line == "" {
				continue
			}
			i := strings.LastIndexByte(line, ' ')
			if i < 0 {
				continue
			}
			if ts, err := strconv.ParseInt(strings.TrimSpace(line[i+1:]), 10, 64); err == nil {
				tsCount[ts]++
				totalLines.Add(1)
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(tgt.Close)

	_, addr := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq"))
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ic, err := influx.NewClient(influx.Config{URL: src.URL, Database: "power", Timeout: "3s"})
	if err != nil {
		t.Fatal(err)
	}
	metrics := monitor.New()
	poller := sender.NewPoller(ic, w, metrics, testLogger(t), sender.PollerConfig{
		Interval: 50 * time.Millisecond, Window: 500 * time.Millisecond, Watermark: 300 * time.Millisecond,
		BatchPoints: 100,
	})
	// A4：mode=on 强制转发（状态机由单测覆盖）
	fp := sender.NewFastPath(w, metrics, testLogger(t), sender.FastPathConfig{Mode: sender.FastPathOn}, poller.Notify, nil)
	poller.SetFastPath(fp)

	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 2 * time.Second})
	sl := sender.NewSender(w, client, metrics, testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 20 * time.Millisecond, BackoffMax: 200 * time.Millisecond,
		IdleSleep: 5 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 游标初始化到近实时（对齐 cmd/sender 首次启动行为：now - watermark - 余量）
	if err := w.SetCursor(time.Now().Add(-time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	go poller.Run(ctx)
	go sl.Run(ctx)

	// 推送 3 个点：网格对齐且位于 (cursor, now-watermark) 区间内——轮询窗口必然
	// 返回孪生行（去重路径确定性触发）。base-3..-1 步 ≈ now-400ms..now-200ms。
	now := time.Now().UnixNano()
	base := now / step * step
	pushed := []int64{base - 3*step, base - 2*step, base - step}
	var body strings.Builder
	for _, ts := range pushed {
		fmt.Fprintf(&body, "telemetry,plant=A01 value=9 %d\n", ts)
	}
	rec := httptest.NewRecorder()
	fp.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body.String())))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("fast path code=%d", rec.Code)
	}
	if metrics.FastPathPoints() != 3 {
		t.Fatalf("fast path points=%d", metrics.FastPathPoints())
	}

	// 等待：WAL 清空 + 去重命中 3（轮询窗口已覆盖推送点）
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if metrics.FastPathDedupHit() >= 3 && w.PendingCount() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if metrics.FastPathDedupHit() != 3 {
		t.Fatalf("dedup hits=%d, want 3 (poller must suppress twin rows)", metrics.FastPathDedupHit())
	}
	if w.PendingCount() != 0 {
		t.Fatalf("wal pending=%d", w.PendingCount())
	}
	// 零重复：每个推送点在目标库恰好出现一次（快路径副本；轮询孪生被去重）
	mu.Lock()
	for _, ts := range pushed {
		if got := tsCount[ts]; got != 1 {
			mu.Unlock()
			t.Fatalf("pushed ts=%d appeared %d times in target, want exactly 1", ts, got)
		}
	}
	mu.Unlock()
	// 零丢失：背景轮询数据持续落库
	if totalLines.Load() < int64(len(pushed)) {
		t.Fatalf("total lines=%d", totalLines.Load())
	}
}

// TestEndToEndBackfillAllV17 V1.7 语义 e2e：
// ① 新装 backfill=all：游标=库内最早数据，全量同步（含比"现在"早 2 分钟的旧数据）；
// ② 追平后同值重启不回拨；③ 配置变化（all→0→all）重新回拨，目标库计数不重复（幂等）。
func TestEndToEndBackfillAllV17(t *testing.T) {
	const step = int64(100_000_000) // 100ms 网格
	now := time.Now().UnixNano()
	// SHOW SHARD GROUPS 的 start_time 为秒级 RFC3339：对齐整秒避免亚秒差异
	oldest := (now - 40*time.Second.Nanoseconds()) / int64(time.Second) * int64(time.Second)
	oldest = oldest / step * step
	dataEnd := (now - 2*time.Second.Nanoseconds()) / step * step // 数据写到 now-2s 为止（边界之外查询返回空）
	var seq atomic.Int64
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW SHARD GROUPS") {
			st := time.Unix(0, oldest).UTC().Format(time.RFC3339)
			fmt.Fprintf(w, `{"results":[{"series":[{"name":"power","columns":["id","database","retention_policy","shard_group","start_time","end_time","expiry_time"],"values":[[1,"power","autogen",1,%q,"2026-12-31T00:00:00Z","2027-01-07T00:00:00Z"]]}]}]}`, st)
			return
		}
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"telemetry","columns":["tagKey"],"values":[["plant"]]}]}]}`)
			return
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"telemetry","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		var start, end int64
		if _, err := fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end); err != nil {
			fmt.Sscanf(q, "SELECT * FROM \"telemetry\" WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
		}
		if end > dataEnd {
			end = dataEnd
		}
		var rows []string
		first := (start + step - 1) / step * step
		for ts := first; ts < end; ts += step {
			rows = append(rows, fmt.Sprintf(`[%d,"A01",%d]`, ts, seq.Add(1)))
		}
		fmt.Fprintf(w, `{"results":[{"series":[{"name":"telemetry","columns":["time","plant","value"],"values":[%s]}]}]}`, strings.Join(rows, ","))
	}))
	t.Cleanup(src.Close)
	// 目标库：模拟真实 InfluxDB 的幂等 upsert——同一 (series, ts) 重复写入只计一次。
	// （真实目标库重复写不重复计数，重爬历史的幂等性必须在此语义下验证）
	var targetLines atomic.Int64
	var seenMu sync.Mutex
	seen := map[int64]struct{}{}
	tgt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		seenMu.Lock()
		for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if l == "" {
				continue
			}
			i := strings.LastIndexByte(l, ' ')
			if i < 0 {
				continue
			}
			if ts, err := strconv.ParseInt(strings.TrimSpace(l[i+1:]), 10, 64); err == nil {
				if _, dup := seen[ts]; !dup {
					seen[ts] = struct{}{}
					targetLines.Add(1)
				}
			}
		}
		seenMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(tgt.Close)
	rs, addr := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq"))
	_ = rs

	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	ic, err := influx.NewClient(influx.Config{URL: src.URL, Database: "power", Timeout: "5s"})
	if err != nil {
		t.Fatal(err)
	}
	metrics := monitor.New()
	poller := sender.NewPoller(ic, w, metrics, testLogger(t), sender.PollerConfig{
		Interval: 50 * time.Millisecond, Window: 500 * time.Millisecond, Watermark: 1 * time.Second,
		BatchPoints: 500,
	})
	client := transport.NewClient(transport.ClientConfig{Addr: addr, Timeout: 3 * time.Second})
	sl := sender.NewSender(w, client, metrics, testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 模拟 cmd/sender V1.7 新装流程：探测最早数据 → 应用 all 策略 → 游标=最早数据
	oldestProbe, err := ic.ProbeOldestData(ctx)
	if err != nil || oldestProbe != oldest {
		t.Fatalf("probe=%d err=%v want %d", oldestProbe, err, oldest)
	}
	if _, err := w.ApplyBackfillPolicy(wal.BackfillAllNs, oldestProbe); err != nil {
		t.Fatal(err)
	}
	if err := w.SetCursor(oldestProbe); err != nil {
		t.Fatal(err)
	}
	go poller.Run(ctx)
	go sl.Run(ctx)

	// ① 全量回填：以目标库精确计数收敛（已知数据总量，等收到全量且 WAL 排空）
	wantTotal := (dataEnd - oldest) / step // 半开区间 [start,end)：不含 dataEnd 本身
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if targetLines.Load() >= wantTotal && w.PendingCount() == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := targetLines.Load(); got != wantTotal {
		t.Fatalf("backfill all: target=%d want=%d (cursor=%d)", got, wantTotal, w.Cursor())
	}
	cancel()
	time.Sleep(200 * time.Millisecond)
	w.Close()

	// ② 同值重启：不回拨（cursor 保持在追平位置）
	w2, err := wal.Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	cursorBefore := w2.Cursor()
	rewound, err := w2.ApplyBackfillPolicy(wal.BackfillAllNs, oldestProbe)
	if err != nil || rewound || w2.Cursor() != cursorBefore {
		t.Fatalf("same-value restart must not rewind: rewound=%v cursor %d->%d", rewound, cursorBefore, w2.Cursor())
	}

	// ③ 配置变化：all→0→all 触发重新回拨；重发历史幂等（目标计数不变）
	if _, err := w2.ApplyBackfillPolicy(wal.BackfillNoneNs, 0); err != nil {
		t.Fatal(err)
	}
	rewound, err = w2.ApplyBackfillPolicy(wal.BackfillAllNs, oldestProbe)
	if err != nil || !rewound {
		t.Fatalf("config change must rewind: rewound=%v err=%v", rewound, err)
	}
	if w2.Cursor() != oldestProbe {
		t.Fatalf("rewound cursor=%d want %d", w2.Cursor(), oldestProbe)
	}
	w2.Close()
	// 重爬一遍（用新 sender/receiver 会话走完）：目标库计数不重复
	rs2, addr2 := startReceiverSrv(t, tgt.URL, filepath.Join(t.TempDir(), "last_seq2"))
	_ = rs2
	client2 := transport.NewClient(transport.ClientConfig{Addr: addr2, Timeout: 3 * time.Second})
	sl2 := sender.NewSender(w2, client2, monitor.New(), testLogger(t), sender.SenderConfig{
		MaxRetry: 5, BackoffBase: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond, HeartbeatInterval: time.Hour,
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	poller2 := sender.NewPoller(ic, w2, monitor.New(), testLogger(t), sender.PollerConfig{
		Interval: 50 * time.Millisecond, Window: 500 * time.Millisecond, Watermark: 1 * time.Second,
		BatchPoints: 500,
	})
	go poller2.Run(ctx2)
	go sl2.Run(ctx2)
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if w2.Cursor() >= dataEnd-int64(time.Second) && w2.PendingCount() == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel2()
	if got := targetLines.Load(); got != wantTotal {
		t.Fatalf("re-backfill must be idempotent: target=%d want=%d", got, wantTotal)
	}
	w2.Close()
}
