// Package integration 端到端集成测试：完整同步链路 + 断连恢复。
package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
