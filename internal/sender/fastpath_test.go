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
	// V1.7.1：去重集为时间基准驱逐——用"当前"时间戳（源库实时写入的常态）；
	// 旧固定时间戳（2024 年）会因远超 retention 被立即驱逐（驱逐只造成重复转发，安全）
	base := time.Now().UnixNano()
	rec := pushBody(t, fp, fmt.Sprintf("m,t=a value=1 %d\nm,t=b value=2 %d\n", base, base+1))
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
		{"m", map[string]string{"t": "a"}, base},
		{"m", map[string]string{"t": "b"}, base + 1},
	}
	filtered := fp.Filter([]model.Point{
		{Measurement: pts[0].meas, Tags: pts[0].tags, Timestamp: pts[0].ts},
		{Measurement: pts[1].meas, Tags: pts[1].tags, Timestamp: pts[1].ts},
		{Measurement: "m", Tags: map[string]string{"t": "c"}, Timestamp: base + 2},
	})
	if len(filtered) != 1 || filtered[0].Timestamp != base+2 {
		t.Fatalf("filtered=%+v", filtered)
	}
	if m.FastPathDedupHit() != 2 {
		t.Fatalf("dedup hits=%d", m.FastPathDedupHit())
	}
}

// TestFastPathImmediateActive V1.7：auto/on 启用即转发——即使游标落后 30 天（回填中），
// 实时数据也立即透传，不再等追平；off 仍为仅信号。
func TestFastPathImmediateActive(t *testing.T) {
	cfg := FastPathConfig{Mode: FastPathAuto}
	fp, w, m, _, done := newTestFastPath(t, cfg)
	defer done()
	// 游标落后 30 天（历史回填中）：仍应立即转发
	fp.SetCursor(time.Now().Add(-30 * 24 * time.Hour).UnixNano())
	if !fp.Active() {
		t.Fatal("auto mode must be active immediately (no catch-up gate)")
	}
	pushBody(t, fp, "m value=1 1720000000000000000\n")
	if w.PendingCount() != 1 || m.FastPathPoints() != 1 {
		t.Fatalf("active: pending=%d pts=%d", w.PendingCount(), m.FastPathPoints())
	}
	if m.FastPathSignalOnly() != 0 {
		t.Fatalf("sig=%d, want 0", m.FastPathSignalOnly())
	}
}

// TestFastPathOnModeForward V1.7：on 与 auto 行为一致（立即转发）。
func TestFastPathOnModeForward(t *testing.T) {
	fp, w, m, _, done := newTestFastPath(t, FastPathConfig{Mode: FastPathOn})
	defer done()
	fp.SetCursor(time.Now().Add(-time.Hour).UnixNano())
	if !fp.Active() {
		t.Fatal("on mode must be active")
	}
	pushBody(t, fp, "m value=1 1720000000000000000\n")
	if w.PendingCount() != 1 || m.FastPathPoints() != 1 {
		t.Fatalf("pending=%d pts=%d", w.PendingCount(), m.FastPathPoints())
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

// TestFastDedupBoundedDuringBackfill N9 回归：回填期（游标远落后）快路径持续登记
// 实时点，去重集必须时间基准驱逐、内存有界——修复前按游标驱逐会滞留整个回填
// 期的全部条目（200k/s×24h ≈ TB 级 → OOM）。
func TestFastDedupBoundedDuringBackfill(t *testing.T) {
	d := newFastDedup(15 * time.Second)
	// 游标停留在 30 天前（回填中），快路径持续登记"实时"点（ts≈now）
	oldCursor := time.Now().Add(-30 * 24 * time.Hour).UnixNano()
	d.SetCursor(oldCursor)
	now := time.Now().UnixNano()
	// 模拟 1 小时内的实时点（每 10ms 一点 ≈ 360k 点）
	start := now - int64(time.Hour)
	for ts := start; ts < now; ts += 10_000_000 {
		d.Add("m|s=1|", ts)
	}
	// 修复后：条目必须被时间基准驱逐到 retention 窗口内（≤ retention/10ms ≈ 1500 条）
	if n := d.Len(); n > 5000 {
		t.Fatalf("dedup unbounded during backfill: %d entries (want ≤ ~5000)", n)
	}
	// 且近 retention 内的点仍可命中（去重有效性未受损）
	if !d.Contains("m|s=1|", now-5*int64(time.Second)) {
		t.Fatal("recent entry must still be dedupable")
	}
	// 早已过 retention 的点被驱逐（驱逐只会造成重复转发，零丢失）
	if d.Contains("m|s=1|", now-int64(time.Hour)) {
		t.Fatal("expired entry must be evicted")
	}
}

// TestFastDedupSetCursorEviction 游标基准驱逐仍是安全下界（ts<cursor-retention 永不回查）。
func TestFastDedupSetCursorEviction(t *testing.T) {
	d := newFastDedup(15 * time.Second)
	now := time.Now().UnixNano()
	d.Add("m|s=1|", now-60*int64(time.Second))
	d.Add("m|s=1|", now)
	d.SetCursor(now) // 游标前进到 now：60s 前的条目无意义
	if d.Contains("m|s=1|", now-60*int64(time.Second)) {
		t.Fatal("entry older than cursor-retention must be evicted")
	}
	if !d.Contains("m|s=1|", now) {
		t.Fatal("recent entry must remain")
	}
}

// TestFastDedupSeriesBounded R1 回归：病态高基数（tag 近似唯一 ID）下 series
// 映射随条目驱逐惰性清理——最后一个条目被驱逐后 series 名同步回收，不无界增长。
func TestFastDedupSeriesBounded(t *testing.T) {
	d := newFastDedup(15 * time.Second)
	now := time.Now().UnixNano()
	const n = 10000
	for i := 0; i < n; i++ {
		d.Add(fmt.Sprintf("m|id=%d|", i), now)
	}
	d.mu.Lock()
	if len(d.series) != n {
		t.Fatalf("series=%d, want %d", len(d.series), n)
	}
	if len(d.idRefs) != n {
		t.Fatalf("refs=%d, want %d", len(d.idRefs), n)
	}
	// 全部条目驱逐后：series 名必须被惰性清理（R1）
	d.evictLocked(time.Now().Add(2 * d.retention).UnixNano())
	left := len(d.series)
	d.mu.Unlock()
	if left != 0 {
		t.Fatalf("series map not cleaned after eviction: %d entries left", left)
	}
	if d.Len() != 0 {
		t.Fatalf("dedup entries=%d, want 0", d.Len())
	}
	// 清理后 Contains 正确返回 false（去重退化=重复转发，零丢失语义不变）
	if d.Contains("m|id=1|", now) {
		t.Fatal("Contains must be false after cleanup")
	}
}

// TestFastDedupSeriesRefcountKeepsActive 引用计数：同 series 跨分区时，驱逐旧分区
// 不得误删仍有活跃条目的 series。
func TestFastDedupSeriesRefcountKeepsActive(t *testing.T) {
	d := newFastDedup(15 * time.Second)
	now := time.Now().UnixNano()
	d.Add("m|s=1|", now-20*int64(time.Second)) // 旧分区（超出 retention）
	d.Add("m|s=1|", now)                        // 新分区（同 series）
	d.SetCursor(now)                            // 游标推进：驱逐旧分区（cut=now-retention 之前）
	d.mu.Lock()
	_, ok := d.series["m|s=1|"]
	d.mu.Unlock()
	if !ok {
		t.Fatal("series must be kept while new partition entry is alive")
	}
	if !d.Contains("m|s=1|", now) {
		t.Fatal("recent entry must remain dedupable")
	}
	if d.Contains("m|s=1|", now-20*int64(time.Second)) {
		t.Fatal("old entry must be evicted")
	}
}
