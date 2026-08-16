// Package sender A4 订阅 fast-path：接收源库 SUBSCRIPTION 推送的原始 Line Protocol
// 批次，直接透传进同一 WAL/seq 流，把端到端延迟从 watermark+处理(~4.2s) 降到
// flush 间隔+传输(~0.1-1s)。轮询路径（Poller）保持为零丢失正确性基座：快路径的
// 一切退化方向（订阅丢包/解析跳过/反压丢批/去重集驱逐）都只造成"重复转发"或
// "退回轮询"，不存在丢数据方向。
package sender

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/model"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/wal"
)

// FastPathMode 快路径模式：off=仅信号 / auto、on=启用即透传（V1.7 起 auto≡on，
// 不再等待游标追平——历史回填与实时透传并行，实时同步"打开就生效"）。
type FastPathMode string

const (
	FastPathOff  FastPathMode = "off"
	FastPathAuto FastPathMode = "auto"
	FastPathOn   FastPathMode = "on"
)

// FastPathConfig 快路径配置。
type FastPathConfig struct {
	Mode          FastPathMode  // off=仅信号 / on、auto（≡on，V1.7 起不再等游标追平）=立即透传
	DedupWindow   time.Duration // 去重集保留窗口（默认 watermark+5s）
	Measurements  []string      // 同步 measurement 白名单（与轮询路径一致；空=全部）
	MaxBatchBytes int           // 单批上限（默认 MaxDecompressedLen）
	Compression   uint8         // 数据帧类型（protocol.TypeData=gzip / TypeDataZstd=zstd），默认 zstd
}

// ns 精度守卫：ts 必须落在 [1e15, 5e18)（1970-09 至 2128）。ns/µs 数值域重叠
// （1.75e15 既是 ns-1970 也是 µs-2025）无法可靠判别，非 ns 批次跳过由 Poller 兜底
// （Poller 经 epoch=ns 查询拿到正确精度）。
const (
	minNsTimestamp = int64(1e15)
	maxNsTimestamp = int64(5e18)
)

// fastDedup 快路径→轮询路径的转发去重集（A4）。
//
// 结构：秒级分区 + series 紧凑 ID。条目 = id<<30 | (ts % 1e9)，精确键零碰撞。
// 登记时机：仅在 WAL AppendBatch **成功之后**——保证"被 Poller 跳过 ⟹ 已实际
// 转发"（零丢失证明的核心）。驱逐：分区秒 < cursor 即删（Poller 永不回查）。
// 驱逐/重启丢失条目只会造成重复转发（目标库幂等覆盖），安全方向唯一。
type fastDedup struct {
	mu        sync.Mutex
	series    map[string]uint64
	idName    []string       // id -> series 名（逆向查找，驱逐清理用）
	idRefs    map[uint64]int // id -> 活跃条目引用计数（R1：最后一个条目驱逐时惰性清理 series）
	nextID    uint64
	parts     map[int64]map[uint64]struct{} // 秒 -> packed(id, offset)
	retention time.Duration
	cursorNs  atomic.Int64 // 最近一次 Poller 游标（驱逐基准）
}

func newFastDedup(retention time.Duration) *fastDedup {
	if retention <= 0 {
		retention = 15 * time.Second
	}
	return &fastDedup{
		series:    make(map[string]uint64),
		parts:     make(map[int64]map[uint64]struct{}),
		retention: retention,
	}
}

// SetCursor 更新驱逐基准（Poller 每轮 poll 调用，值为本轮查询窗口起点）。
// 基于游标的驱逐是安全的下界优化（ts < cursor-retention 的点 Poller 永不回查）；
// 真正的内存有界由 Add 中的时间基准驱逐保证（见 evictExpiredLocked 注释）。
func (d *fastDedup) SetCursor(cursorNs int64) {
	d.cursorNs.Store(cursorNs)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.evictLocked(cursorNs - int64(d.retention))
}

// evictLocked 删除 sec < cutSec 的分区（调用方持锁）。
// R1：驱逐条目时按 ID 引用计数回退，某 series 的最后一个条目被驱逐后
// 惰性清理其 series 名——病态高基数场景（tag 近似唯一 ID）下 series 映射
// 也不再无界增长。
func (d *fastDedup) evictLocked(cutNs int64) {
	cut := cutNs / int64(time.Second)
	for sec, part := range d.parts {
		if sec >= cut {
			continue
		}
		for packed := range part {
			id := packed >> 30
			if n := d.idRefs[id] - 1; n > 0 {
				d.idRefs[id] = n
			} else {
				// 该 series 已无活跃条目：回收 ID 名称映射
				if int(id) < len(d.idName) && d.idName[id] != "" {
					delete(d.series, d.idName[id])
					d.idName[id] = ""
				}
				delete(d.idRefs, id)
			}
		}
		delete(d.parts, sec)
	}
}

// Add 登记一个已转发点（调用方保证 WAL 已成功落盘）。
// V1.7.1：每次登记同时做**时间基准**驱逐（now-retention 之前的分区删除）。
// 背景：回填期（backfill=all）游标在历史区慢爬，而快路径持续转发实时点
// （ts 远超前于游标）——仅靠游标驱逐会导致整个回填期（天级）的条目全部
// 滞留：200k/s × 24h ≈ 170 亿条目 ≈ TB 级内存，必 OOM。时间基准驱逐把内存
// 上界钉在 retention×rate（默认 15s×200k ≈ 300 万条）；被提前驱逐的条目
// 退化方向唯一：重复转发（目标库幂等覆盖），零丢失。
func (d *fastDedup) Add(seriesKey string, ts int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.evictLocked(time.Now().UnixNano() - int64(d.retention))
	id, ok := d.series[seriesKey]
	if !ok {
		if d.nextID >= 1<<34 {
			return // series 数超设计上限：拒绝登记（去重退化为重复转发，安全）
		}
		id = d.nextID
		d.nextID++
		d.series[seriesKey] = id
		for len(d.idName) <= int(id) {
			d.idName = append(d.idName, "")
		}
		d.idName[id] = seriesKey
	}
	sec := ts / int64(time.Second)
	part := d.parts[sec]
	if part == nil {
		part = make(map[uint64]struct{})
		d.parts[sec] = part
	}
	part[id<<30|uint64(ts-sec*int64(time.Second))] = struct{}{}
	if d.idRefs == nil {
		d.idRefs = make(map[uint64]int)
	}
	d.idRefs[id]++
}

// Contains 查询点是否已由快路径转发。
func (d *fastDedup) Contains(seriesKey string, ts int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	id, ok := d.series[seriesKey]
	if !ok {
		return false
	}
	sec := ts / int64(time.Second)
	part := d.parts[sec]
	if part == nil {
		return false
	}
	_, hit := part[id<<30|uint64(ts-sec*int64(time.Second))]
	return hit
}

// Len 返回集内条目数（指标用）。
func (d *fastDedup) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, p := range d.parts {
		n += len(p)
	}
	return n
}

// FastPath A4 快路径转发器（HTTP handler）。线程安全（并发推送）。
type FastPath struct {
	wal      *wal.WAL
	dedup    *fastDedup
	cfg      FastPathConfig
	metrics  *monitor.Metrics
	logger   *zap.Logger
	notify   func()         // Poller 信号回调（批处理成功后唤醒 Poller）
	diskGate func() float64 // WAL 盘占用率（反压门）

	active atomic.Bool // 是否转发（V1.7：启用即真，off 恒假——不再有游标年龄门控）
}

// NewFastPath 创建快路径转发器。
func NewFastPath(w *wal.WAL, metrics *monitor.Metrics, logger *zap.Logger, cfg FastPathConfig, notify func(), diskGate func() float64) *FastPath {
	if cfg.Mode == "" {
		cfg.Mode = FastPathAuto
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = protocol.MaxDecompressedLen
	}
	if cfg.Compression == 0 {
		cfg.Compression = protocol.TypeDataZstd
	}
	fp := &FastPath{
		wal:      w,
		dedup:    newFastDedup(cfg.DedupWindow),
		cfg:      cfg,
		metrics:  metrics,
		logger:   logger,
		notify:   notify,
		diskGate: diskGate,
	}
	// V1.7：启用即转发（off 除外）——实时同步"打开就生效"，与历史回填并行互不阻塞。
	if cfg.Mode != FastPathOff {
		fp.active.Store(true)
	}
	return fp
}

// Active 返回当前是否处于转发状态（指标/测试用）。
func (f *FastPath) Active() bool { return f.active.Load() }

// Enabled 配置是否启用（listen 已配置）。
func (f *FastPath) Enabled() bool { return f.cfg.Mode != FastPathOff }

// SetCursor 由 Poller 每轮调用：更新去重集驱逐基准（V1.7：仅此职责，无状态机）。
func (f *FastPath) SetCursor(cursorNs int64) {
	f.dedup.SetCursor(cursorNs)
}

// Filter 供 Poller 在查询归并后调用：剔除已由快路径转发的点。
// 跳过 ⟹ 已转发（登记在 WAL 成功之后），零丢失。
func (f *FastPath) Filter(points []model.Point) []model.Point {
	if len(points) == 0 {
		return points
	}
	out := points[:0]
	for i := range points {
		key := model.SeriesKey(points[i].Measurement, points[i].Tags)
		if f.dedup.Contains(key, points[i].Timestamp) {
			f.metrics.IncFastPathDedupHit()
			continue
		}
		out = append(out, points[i])
	}
	return out
}

// ServeHTTP 处理一次订阅推送批次。
func (f *FastPath) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.metrics.IncFastPathBatch()
	if f.notify != nil {
		f.notify() // 无论是否转发，到达即信号（复用现有合并语义）
	}
	if f.cfg.Mode == FastPathOff || !f.active.Load() {
		f.metrics.IncFastPathSignalOnly()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(f.cfg.MaxBatchBytes)+1))
	if err != nil || len(body) == 0 {
		f.metrics.IncFastPathDroppedOversize()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(body) > f.cfg.MaxBatchBytes {
		f.metrics.IncFastPathDroppedOversize()
		f.logger.Warn("fast path batch over size limit, skipped (polling covers)",
			zap.Int("bytes", len(body)), zap.Int("limit", f.cfg.MaxBatchBytes))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// 反压门：WAL 盘占用 ≥ 黄线时丢批（Poller 兜底，延迟退化回轮询）
	if gate := f.diskGate; gate != nil && gate() >= bpYellowTrigger {
		f.metrics.IncFastPathDroppedBackpressure()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// 逐行解析过滤：坏行/非 ns 行/非目标 measurement 行跳过（Poller 兜底）
	var out []byte
	var keys []dedupKey
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		meas, tags, ts, ok := model.ParseLine(line)
		if !ok {
			f.metrics.IncFastPathLineSkipped()
			continue
		}
		if ts < minNsTimestamp || ts > maxNsTimestamp {
			f.metrics.IncFastPathLineSkipped()
			continue // 非 ns 精度：跳过，Poller 经 epoch=ns 正确处理
		}
		if !f.measurementAllowed(meas) {
			f.metrics.IncFastPathLineSkipped()
			continue
		}
		out = append(out, line...)
		out = append(out, '\n')
		keys = append(keys, dedupKey{series: model.SeriesKeyFromPairs(meas, tags), ts: ts})
	}
	if len(out) == 0 {
		f.metrics.IncFastPathDroppedPrecision()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// 组帧（压缩与轮询路径一致：gzip/zstd 按配置）→ WAL（内部分配 seq）→ 成功后登记去重
	fb, err := protocol.Encode(f.cfg.Compression, 0, out)
	if err != nil {
		// 压缩后超 1MB 帧限等：整批跳过（Poller 兜底）
		f.metrics.IncFastPathDroppedOversize()
		f.logger.Warn("fast path batch encode failed, skipped", zap.Error(err))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if _, err := f.wal.AppendBatch(protocol.TypeData, [][]byte{fb}); err != nil {
		f.metrics.IncFastPathDroppedBackpressure()
		f.logger.Warn("fast path wal append failed, skipped (polling covers)", zap.Error(err))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	for i := range keys {
		f.dedup.Add(keys[i].series, keys[i].ts)
	}
	f.metrics.AddFastPathPoints(int64(len(keys)))
	w.WriteHeader(http.StatusNoContent)
}

type dedupKey struct {
	series string
	ts     int64
}

// measurementAllowed 判断 measurement 是否在同步白名单内（空=全部）。
func (f *FastPath) measurementAllowed(meas string) bool {
	if len(f.cfg.Measurements) == 0 {
		return true
	}
	for _, m := range f.cfg.Measurements {
		if m == meas {
			return true
		}
	}
	return false
}
