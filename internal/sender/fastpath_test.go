package sender

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/model"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/wal"
)

func newTestFastPath(t *testing.T, cfg FastPathConfig) (*FastPath, *wal.WAL, *monitor.Metrics, *httptest.ResponseRecorder, func()) {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	notify := func() {}
	fp := NewFastPath(w, m, zap.NewNop(), cfg, notify, nil)
	return fp, w, m, httptest.NewRecorder(), func() { w.Close() }
}

func pushBody(t *testing.T, fp *FastPath, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	fp.ServeHTTP(rec, req)
	return rec
}

func TestFastPathOffIsSignalOnly(t *testing.T) {
	fp, w, m, _, done := newTestFastPath(t, FastPathConfig{Mode: FastPathOff})
	defer done()
	rec := pushBody(t, fp, "m value=1 1720000000000000000\n")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("off mode must not append, pending=%d", w.PendingCount())
	}
	if m.FastPathSignalOnly() != 1 || m.FastPathPoints() != 0 {
		t.Fatalf("sig=%d pts=%d", m.FastPathSignalOnly(), m.FastPathPoints())
	}
}

func TestFastPathOnForwards(t *testing.T) {
	fp, w, m, _, done := newTestFastPath(t, FastPathConfig{Mode: FastPathOn})
	defer done()
	rec := pushBody(t, fp, "m,t=a value=1 1720000000000000000\nm,t=b value=2 1720000000000000001\n")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}
	if w.PendingCount() != 1 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	if m.FastPathPoints() != 2 {
		t.Fatalf("pts=%d", m.FastPathPoints())
	}
	// 去重集已登记：Poller 过滤应跳过这两个点
	pts := []struct {
		meas string
		tags map[string]string
		ts   int64
	}{
		{"m", map[string]string{"t": "a"}, 1720000000000000000},
		{"m", map[string]string{"t": "b"}, 1720000000000000001},
	}
	filtered := fp.Filter([]model.Point{
		{Measurement: pts[0].meas, Tags: pts[0].tags, Timestamp: pts[0].ts},
		{Measurement: pts[1].meas, Tags: pts[1].tags, Timestamp: pts[1].ts},
		{Measurement: "m", Tags: map[string]string{"t": "c"}, Timestamp: 1720000000000000002},
	})
	if len(filtered) != 1 || filtered[0].Timestamp != 1720000000000000002 {
		t.Fatalf("filtered=%+v", filtered)
	}
	if m.FastPathDedupHit() != 2 {
		t.Fatalf("dedup hits=%d", m.FastPathDedupHit())
	}
}

// TestFastPathAutoStateMachine auto 模式：游标落后 → 仅信号；追平 → 透传；再落后 → 退回仅信号。
func TestFastPathAutoStateMachine(t *testing.T) {
	cfg := FastPathConfig{Mode: FastPathAuto, ActivateAge: 5 * time.Second, DeactivateAge: 30 * time.Second}
	fp, w, m, _, done := newTestFastPath(t, cfg)
	defer done()
	// 游标落后 1 小时：WAITING
	old := time.Now().Add(-time.Hour).UnixNano()
	fp.SetCursor(old)
	if fp.Active() {
		t.Fatal("must be waiting while cursor lags")
	}
	pushBody(t, fp, "m value=1 1720000000000000000\n")
	if w.PendingCount() != 0 || m.FastPathSignalOnly() != 1 {
		t.Fatalf("waiting: pending=%d sig=%d", w.PendingCount(), m.FastPathSignalOnly())
	}
	// 追平：ACTIVE
	fp.SetCursor(time.Now().Add(-time.Second).UnixNano())
	if !fp.Active() {
		t.Fatal("must activate after catch-up")
	}
	pushBody(t, fp, "m value=2 1720000000000000000\n")
	if w.PendingCount() != 1 || m.FastPathPoints() != 1 {
		t.Fatalf("active: pending=%d pts=%d", w.PendingCount(), m.FastPathPoints())
	}
	// 再次落后（迟滞阈值之外）：退回 WAITING
	fp.SetCursor(time.Now().Add(-time.Hour).UnixNano())
	if fp.Active() {
		t.Fatal("must deactivate after lag")
	}
	pushBody(t, fp, "m value=3 1720000000000000000\n")
	if w.PendingCount() != 1 || m.FastPathSignalOnly() != 2 {
		t.Fatalf("deactivated: pending=%d sig=%d", w.PendingCount(), m.FastPathSignalOnly())
	}
}

// TestFastPathAutoHysteresis 迟滞：阈值附近小幅抖动不翻转。
func TestFastPathAutoHysteresis(t *testing.T) {
	cfg := FastPathConfig{Mode: FastPathAuto, ActivateAge: 5 * time.Second, DeactivateAge: 30 * time.Second}
	fp, _, _, _, done := newTestFastPath(t, cfg)
	defer done()
	fp.SetCursor(time.Now().Add(-time.Second).UnixNano()) // 激活
	if !fp.Active() {
		t.Fatal("must activate")
	}
	// 年龄回到 10s（>5s 但 <30s）：迟滞区内保持 ACTIVE
	fp.SetCursor(time.Now().Add(-10 * time.Second).UnixNano())
	if !fp.Active() {
		t.Fatal("hysteresis: must stay active within deactivate threshold")
	}
}

func TestFastPathLineFilterAndSkips(t *testing.T) {
	fp, w, m, _, done := newTestFastPath(t, FastPathConfig{
		Mode:         FastPathOn,
		Measurements: []string{"m"},
	})
	defer done()
	body := strings.Join([]string{
		"m,t=a value=1 1720000000000000000",     // 好行
		"other,t=a value=1 1720000000000000001", // 非目标 measurement → 跳过
		"m,t=b value=1 999",                     // 非 ns 精度 → 跳过
		"garbage no ts",                         // 坏行 → 跳过
		"m,t=c value=1 1720000000000000002",     // 好行
	}, "\n")
	rec := pushBody(t, fp, body)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}
	if w.PendingCount() != 1 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	if m.FastPathPoints() != 2 || m.FastPathLineSkipped() != 3 {
		t.Fatalf("pts=%d skipped=%d", m.FastPathPoints(), m.FastPathLineSkipped())
	}
}

func TestFastPathOversizeBody(t *testing.T) {
	fp, w, m, _, done := newTestFastPath(t, FastPathConfig{Mode: FastPathOn, MaxBatchBytes: 10})
	defer done()
	rec := pushBody(t, fp, "m value=1 1720000000000000000\n")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}
	if w.PendingCount() != 0 || m.FastPathDroppedOversize() != 1 {
		t.Fatalf("pending=%d oversize=%d", w.PendingCount(), m.FastPathDroppedOversize())
	}
}

func TestFastPathBackpressureDrops(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	m := monitor.New()
	fp := NewFastPath(w, m, zap.NewNop(), FastPathConfig{Mode: FastPathOn}, func() {}, func() float64 { return 0.9 })
	rec := pushBody(t, fp, "m value=1 1720000000000000000\n")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}
	if w.PendingCount() != 0 || m.FastPathDroppedBackpressure() != 1 {
		t.Fatalf("pending=%d bp=%d", w.PendingCount(), m.FastPathDroppedBackpressure())
	}
	// 丢批绝不登记：Poller 过滤不得跳过该点（零丢失核心）
	filtered := fp.Filter([]model.Point{{Measurement: "m", Timestamp: 1720000000000000000}})
	if len(filtered) != 1 {
		t.Fatal("dropped batch must not be registered for dedup (would lose data)")
	}
}

// TestFastPathAppendFailureNeverRegisters WAL 追加失败时不得登记去重（零丢失证明）。
func TestFastPathAppendFailureNeverRegisters(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	m := monitor.New()
	fp := NewFastPath(w, m, zap.NewNop(), FastPathConfig{Mode: FastPathOn}, func() {}, nil)
	w.Close()
	os.RemoveAll(filepath.Join(dir, "wal")) // WAL 目录消失 → 追加必失败
	rec := pushBody(t, fp, "m value=1 1720000000000000000\n")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}
	if m.FastPathDroppedBackpressure() == 0 {
		t.Fatal("append failure must be counted")
	}
	filtered := fp.Filter([]model.Point{{Measurement: "m", Timestamp: 1720000000000000000}})
	if len(filtered) != 1 {
		t.Fatal("failed append must never be dedup-registered (would lose data)")
	}
}

// TestFastDedupPrune 驱逐只造成"不命中"（重复转发方向），绝不造成错误命中。
func TestFastDedupPrune(t *testing.T) {
	d := newFastDedup(5 * time.Second)
	ts := time.Now().UnixNano()
	key := "m|t=a|"
	d.Add(key, ts)
	if !d.Contains(key, ts) {
		t.Fatal("must contain after add")
	}
	// 游标推进越过保留窗口：驱逐后不命中（安全方向）
	d.SetCursor(ts + int64(30*time.Second))
	if d.Contains(key, ts) {
		t.Fatal("evicted entry must miss (duplicate allowed, skip never)")
	}
	if d.Len() != 0 {
		t.Fatalf("len=%d", d.Len())
	}
}

// TestFastDedupPackedKey 同秒多 series 多 offset 无碰撞。
func TestFastDedupPackedKey(t *testing.T) {
	d := newFastDedup(time.Minute)
	sec := time.Now().UnixNano() / int64(time.Second) * int64(time.Second)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("m|t=%03d|", i)
		d.Add(key, sec+int64(i))
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("m|t=%03d|", i)
		if !d.Contains(key, sec+int64(i)) {
			t.Fatalf("missing key %d", i)
		}
		if d.Contains(key, sec+int64(i)+int64(time.Second)) {
			t.Fatalf("cross-second false hit for key %d", i)
		}
	}
	if d.Contains("m|t=999|", sec) {
		t.Fatal("unknown series must miss")
	}
}

// TestConcurrentAppendBatch A4：Poller 与 FastPath 并发追加，seq 唯一且连续。
func TestConcurrentAppendBatch(t *testing.T) {
	w, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const nG = 8
	const nF = 50
	var wg sync.WaitGroup
	all := make([][]uint64, nG)
	for g := 0; g < nG; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < nF; i++ {
				fb, err := protocol.Encode(protocol.TypeData, 0, []byte(fmt.Sprintf("m v=%d %d", g, i)))
				if err != nil {
					t.Error(err)
					return
				}
				seqs, err := w.AppendBatch(protocol.TypeData, [][]byte{fb})
				if err != nil {
					t.Error(err)
					return
				}
				all[g] = append(all[g], seqs[0])
			}
		}(g)
	}
	wg.Wait()
	// 收集全部 seq：必须恰好是 1..nG*nF 的连续排列
	seen := make(map[uint64]bool)
	for g := 0; g < nG; g++ {
		if len(all[g]) != nF {
			t.Fatalf("goroutine %d frames=%d", g, len(all[g]))
		}
		for _, s := range all[g] {
			if seen[s] {
				t.Fatalf("duplicate seq %d", s)
			}
			seen[s] = true
		}
	}
	for s := uint64(1); s <= uint64(nG*nF); s++ {
		if !seen[s] {
			t.Fatalf("missing seq %d (total %d)", s, len(seen))
		}
	}
	if w.PendingCount() != nG*nF {
		t.Fatalf("pending=%d", w.PendingCount())
	}
}
