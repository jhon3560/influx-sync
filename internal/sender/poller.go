// Package sender 实现 I 区同步发送端：轮询查询 → WAL → 停等发送。
package sender

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/influx"
	"influx-sync/internal/model"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/wal"
)

// PollerConfig 轮询配置。
type PollerConfig struct {
	Interval          time.Duration // 轮询周期，默认 1s
	Window            time.Duration // 查询窗口，默认 5s
	Watermark         time.Duration // 水位延迟，默认 10s
	MaxWindow         time.Duration // 单次查询窗口上限（防时间跳变），默认 30s
	BatchPoints       int           // 单帧点数，默认 10000
	QueryLimit        int           // 单次查询 LIMIT（越大分页越少，扫描开销越低），默认 100000
	ParallelQueries   int           // 多窗口并行查询/组帧 worker 数，默认 4（0/1=串行）
	MinSignalInterval time.Duration // 订阅信号最小查询间隔（防高频信号打满 Poller），默认 200ms
	TagColumns        []string      // 显式 tag 列（空=自动发现）
	Measurements      []string      // 要同步的 measurement 列表（空=全部）
	Compression       uint8         // 数据帧类型（protocol.TypeData=gzip / TypeDataZstd=zstd），默认 zstd
}

// 反压三级水位（《死信隔离与反压机制逻辑》）：
//
//	绿 <60% 全速；黄 60%~80% 降速；红 ≥80% 挂起（迟滞：降至 60% 以下才恢复）
const (
	bpYellowTrigger = 0.60 // 黄色减速阈值
	bpRedTrigger    = 0.80 // 红色熔断阈值
	bpGreenResume   = 0.60 // 迟滞恢复阈值（与黄色触发一致，防止 80% 临界震荡）
)

// bpState 反压状态。
type bpState int

const (
	bpNormal   bpState = iota // 0 绿色全速
	bpDegraded                // 1 黄色降速
	bpPaused                  // 2 红色挂起
)

// Poller 时间窗口轮询器：查询源 Influx → 组帧 → 写 WAL → 推进游标。
// V1.2 支持订阅信号触发：源库 SUBSCRIPTION 推送到达时立即查询。
// V1.4 信号不丢弃：忙时置 pending-flag + MinSignalInterval 短定时器延迟触发
// （信号路径尾部延迟从 ≤1s 降到 ≤200ms），丢失时由兜底 ticker 保证不漏。
type Poller struct {
	client  *influx.Client
	wal     *wal.WAL
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     PollerConfig

	signalCh        chan struct{} // 信号触发通道（由延迟定时器投递）
	signalPending   atomic.Bool   // 有信号待触发（合并语义）
	signalTimer     *time.Timer   // 信号延迟触发定时器（MinSignalInterval 护栏）
	lastPoll        atomic.Int64  // 上次查询时间（unixnano，原子防竞态）
	wakeupScheduled atomic.Bool   // watermark 解锁自唤醒定时器是否已安排（原子，防竞态）

	fastPath *FastPath // A4 快路径（nil=未启用）；查询归并后过滤已转发点

	emptyStreak int // 连续空查询窗口计数（V1.7 回填空窗自适应跳过）
}

// emptySkipMaxWindow 空窗自适应跳过的窗口上限：真空区按 5s→10s→…→1h 翻倍，
// 命中数据即复位。正常数据窗口仍受 MaxWindow(30s) 约束（见 pollOnce）。
const emptySkipMaxWindow = time.Hour

// SetFastPath 注入快路径转发器（A4；nil 时轮询行为与旧版完全一致）。
func (p *Poller) SetFastPath(fp *FastPath) { p.fastPath = fp }

// windowSize 计算本轮查询窗口：连续空窗时翻倍（上限 1h），用于快速越过
// "回拨边界早于库内最早数据"的真空区（正常路径由启动时数据起点探测覆盖，
// 此处兜底运行期出现真空的情况）。
func (p *Poller) windowSize() time.Duration {
	w := p.cfg.Window
	for i := 0; i < p.emptyStreak && w < emptySkipMaxWindow; i++ {
		w *= 2
	}
	if w > emptySkipMaxWindow {
		w = emptySkipMaxWindow
	}
	return w
}

// NewPoller 创建轮询器。
func NewPoller(client *influx.Client, w *wal.WAL, metrics *monitor.Metrics, logger *zap.Logger, cfg PollerConfig) *Poller {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Second
	}
	if cfg.Watermark <= 0 {
		cfg.Watermark = 10 * time.Second
	}
	if cfg.MaxWindow <= 0 {
		cfg.MaxWindow = 30 * time.Second
	}
	if cfg.BatchPoints <= 0 {
		cfg.BatchPoints = 10000
	}
	if cfg.QueryLimit <= 0 {
		cfg.QueryLimit = 100000
	}
	if cfg.ParallelQueries <= 1 {
		cfg.ParallelQueries = 4
	}
	if cfg.MinSignalInterval <= 0 {
		cfg.MinSignalInterval = 200 * time.Millisecond
	}
	if cfg.Compression == 0 {
		cfg.Compression = protocol.TypeDataZstd
	}
	return &Poller{
		client:      client,
		wal:         w,
		metrics:     metrics,
		logger:      logger,
		cfg:         cfg,
		signalCh:    make(chan struct{}, 1),
		signalTimer: time.NewTimer(time.Hour), // 初始长定时器，避免首帧误触发
	}
}

// Notify 订阅信号回调：非阻塞，合并语义。
// 信号不被丢弃：置 pending-flag 并安排 MinSignalInterval 后触发——
// 即使 Poller 忙，短定时器到点后也会补一次查询（延迟 ≤ MinSignalInterval）。
func (p *Poller) Notify() {
	if !p.signalPending.CompareAndSwap(false, true) {
		return // 已有待触发信号：合并
	}
	d := p.cfg.MinSignalInterval - time.Duration(time.Now().UnixNano()-p.lastPoll.Load())
	if d < 0 {
		d = 0
	}
	p.signalTimer.Reset(d)
}

// Run 阻塞运行，直到 ctx 取消。
// 驱动源：订阅信号（优先，事件驱动）+ 兜底 ticker（信号丢失/订阅失效时保底）。
func (p *Poller) Run(ctx context.Context) {
	// 信号延迟定时器：到点投递信号通道（非阻塞，busy 时缓冲待下轮）
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.signalTimer.C:
				p.signalPending.Store(false)
				select {
				case p.signalCh <- struct{}{}:
				default:
				}
			}
		}
	}()
	state := bpNormal
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	pausedStart := time.Time{}
	for {
		triggered := false
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			triggered = true // 兜底轮询：即使无信号也按周期查询（V1.1 行为保留）
		case <-p.signalCh:
			triggered = true // 信号触发（最小间隔已由延迟定时器保证）
		}
		if !triggered {
			continue
		}

		usage := p.walDiskUsageRatio()
		// 迟滞状态机：红区触发挂起，降至绿色阈值以下才恢复（防临界震荡）
		switch state {
		case bpNormal:
			if usage >= bpRedTrigger {
				state = bpPaused
				pausedStart = time.Now()
				p.logger.Warn("backpressure: poller paused", zap.Float64("disk_usage", usage))
			} else if usage >= bpYellowTrigger {
				state = bpDegraded
				p.logger.Warn("backpressure: degraded mode", zap.Float64("disk_usage", usage))
			}
		case bpDegraded:
			if usage >= bpRedTrigger {
				state = bpPaused
				pausedStart = time.Now()
				p.logger.Warn("backpressure: poller paused", zap.Float64("disk_usage", usage))
			} else if usage < bpGreenResume {
				state = bpNormal
			}
		case bpPaused:
			p.metrics.AddPausedSeconds(int64(time.Since(pausedStart).Seconds()))
			pausedStart = time.Now()
			if usage < bpGreenResume {
				state = bpNormal
				p.logger.Info("backpressure: poller resumed", zap.Float64("disk_usage", usage))
			}
		}
		p.metrics.SetBackpressureStatus(int64(state))
		p.metrics.SetWALDiskRatio(usage)

		switch state {
		case bpPaused:
			// 红色熔断：完全挂起，游标冻结，不查源库
			p.metrics.IncPollSkip()
			continue
		case bpDegraded:
			// 黄色降速：间隔拉长至 5s（不阻塞主循环，可响应退出信号）
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}

		blocked := p.pollOnce(ctx)
		p.lastPoll.Store(time.Now().UnixNano())
		// watermark 解锁自唤醒（仅信号模式）：本轮窗口被水位截断时，
		// 安排 watermark 时长后自动再查一次，避免等待下一个 ticker 的闲置延迟
		if blocked && !p.wakeupScheduled.Load() {
			p.wakeupScheduled.Store(true)
			go func() {
				time.Sleep(p.cfg.Watermark)
				p.wakeupScheduled.Store(false)
				p.Notify()
			}()
		}
	}
}

// walDiskUsageRatio 返回 WAL 挂载盘占用率（0~1）；统计失败返回 0（不干预）。
func (p *Poller) walDiskUsageRatio() float64 {
	return WalDiskUsageRatio(p.wal.Dir())
}

// WalDiskUsageRatio 返回指定目录挂载盘占用率（0~1）；统计失败返回 0。
// Poller 反压与 FastPath 反压门共用。
func WalDiskUsageRatio(dir string) float64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0
	}
	total := st.Blocks * uint64(st.Bsize)
	avail := st.Bavail * uint64(st.Bsize)
	if total == 0 {
		return 0
	}
	return float64(total-avail) / float64(total)
}

// pollOnce 执行一轮查询（多窗口并行）。
// 返回 blocked：本轮窗口是否被 watermark 截断（信号模式据此安排解锁自唤醒）。
func (p *Poller) pollOnce(ctx context.Context) (blocked bool) {
	now := time.Now().UnixNano()
	cursor := p.wal.Cursor()
	// V1.7：窗口大小 = 基础窗口 × 空窗翻倍（真空区快速越过）；正常数据窗口
	// 仍受 MaxWindow 上限保护
	window := p.windowSize()
	end := cursor + int64(window)
	if maxEnd := now - int64(p.cfg.Watermark); end > maxEnd {
		end = maxEnd
		blocked = true
	}
	// 窗口上限保护：即使时间跳变/积压，单次最多查 MaxWindow（空窗翻倍除外——
	// 空查询开销小，翻倍上限见 windowSize）
	if p.emptyStreak == 0 && end-cursor > int64(p.cfg.MaxWindow) {
		end = cursor + int64(p.cfg.MaxWindow)
	}
	if end <= cursor {
		return false // 无新窗口
	}

	// V1.7.1：空窗翻倍窗口按 MaxWindow 切片扫描——真空区仍快速越过，但**任何
	// 含数据的切片 ≤ MaxWindow**，单轮内存有界。修复前：连续空窗后首个含数据
	// 窗口可大到 1h（200k/s 下 7.2 亿点一次性入内存 → OOM）。
	if p.emptyStreak > 0 && end-cursor > int64(p.cfg.MaxWindow) {
		return p.pollVacuum(ctx, cursor, end, blocked)
	}

	points, err := p.queryParallel(ctx, cursor, end)
	if err != nil {
		p.logger.Warn("query failed, keep cursor", zap.Error(err))
		return false // 保持游标，下轮重试
	}
	// V1.7：空窗计数——连续空窗翻倍跳过；命中数据复位
	if len(points) == 0 {
		p.emptyStreak++
	} else {
		p.emptyStreak = 0
	}
	// A4：更新快路径去重集驱逐基准（每轮，无论是否过滤）
	if p.fastPath != nil {
		p.fastPath.SetCursor(cursor)
		// 过滤已由快路径转发的点（登记在 WAL 成功后 ⟹ 跳过即已转发，零丢失）
		if before := len(points); before > 0 {
			points = p.fastPath.Filter(points)
			if after := len(points); after < before {
				p.logger.Debug("fast path dedup filtered", zap.Int("skipped", before-after), zap.Int("kept", after))
			}
		}
	}
	p.logger.Info("poll window", zap.Int64("start", cursor), zap.Int64("end", end), zap.Int("points", len(points)))

	// 先写 WAL，成功后才推进游标（顺序铁律，违反会漏数据）
	if err := p.appendFrames(points); err != nil {
		p.logger.Error("wal append failed, keep cursor", zap.Error(err))
		return false
	}
	if err := p.wal.SetCursor(end); err != nil {
		p.logger.Error("cursor update failed", zap.Error(err))
		return false
	}
	p.metrics.SetCursor(end)
	// 每轮刷新 WAL 状态指标（断连积压时可观测）
	p.metrics.SetWALPending(int64(p.wal.PendingCount()))
	p.metrics.SetWALBytes(p.wal.DiskUsage())
	return blocked
}

// pollVacuum 真空区切片扫描（V1.7.1）：在翻倍窗口内按 MaxWindow 逐片查询；
// 空片直接推进游标（跳过），首个含数据片按正常路径处理并返回。
// 空片推进游标安全：查询返回空 ⟹ 该区间确实无数据（若快路径已转发该区间
// 某点，源库必存在该点，查询必返回之，由 Filter 去重）——跳过不丢任何点。
func (p *Poller) pollVacuum(ctx context.Context, cursor, end int64, blocked bool) bool {
	for s := cursor; s < end; {
		e := s + int64(p.cfg.MaxWindow)
		if e > end {
			e = end
		}
		points, err := p.queryParallel(ctx, s, e)
		if err != nil {
			p.logger.Warn("query failed, keep cursor", zap.Error(err))
			return false // 保持游标，下轮重试
		}
		if len(points) > 0 {
			// 命中数据：过滤快路径已转发点后正常处理，空窗计数复位
			p.emptyStreak = 0
			if p.fastPath != nil {
				p.fastPath.SetCursor(s)
				points = p.fastPath.Filter(points)
			}
			p.logger.Info("poll window (vacuum exit)", zap.Int64("start", s), zap.Int64("end", e), zap.Int("points", len(points)))
			if err := p.appendFrames(points); err != nil {
				p.logger.Error("wal append failed, keep cursor", zap.Error(err))
				return false
			}
			if err := p.wal.SetCursor(e); err != nil {
				p.logger.Error("cursor update failed", zap.Error(err))
				return false
			}
			p.metrics.SetCursor(e)
			p.metrics.SetWALPending(int64(p.wal.PendingCount()))
			p.metrics.SetWALBytes(p.wal.DiskUsage())
			return blocked
		}
		// 空片：推进游标（该区间已确认无数据），继续下一片
		p.emptyStreak++
		if p.fastPath != nil {
			p.fastPath.SetCursor(s)
		}
		if err := p.wal.SetCursor(e); err != nil {
			p.logger.Error("cursor update failed", zap.Error(err))
			return false
		}
		p.metrics.SetCursor(e)
		s = e
	}
	p.metrics.SetWALPending(int64(p.wal.PendingCount()))
	p.metrics.SetWALBytes(p.wal.DiskUsage())
	return blocked
}

// queryOptions 组装查询选项。
func (p *Poller) queryOptions() influx.QueryOptions {
	return influx.QueryOptions{
		Measurements: p.cfg.Measurements,
		Limit:        p.cfg.QueryLimit,
		TagColumns:   p.cfg.TagColumns,
	}
}

// queryParallel 将 [start,end) 窗口切分为 N 段并行查询，结果按段序归并。
// 段边界 +1ns 重叠（V1.2.3）：边界点由相邻两段重复查询，防御 InfluxDB
// 高压写入时并行查询偶发漏行。
//
// 去重只覆盖 N-1 个边界时间戳（P2）：重叠仅发生在边界纳秒上，对每个边界
// 收集少量点做比较即可——消除全窗口 Key() 构造与全量 map 内存。
// 并行数<=1 时退化为单次串行查询（行为与 V1.0 一致）。
func (p *Poller) queryParallel(ctx context.Context, start, end int64) ([]model.Point, error) {
	n := p.cfg.ParallelQueries
	if n <= 1 {
		return p.client.QueryRange(ctx, start, end, p.queryOptions())
	}
	step := (end - start) / int64(n)
	if step <= 0 {
		return p.client.QueryRange(ctx, start, end, p.queryOptions())
	}
	type qr struct {
		idx    int
		points []model.Point
		err    error
	}
	results := make(chan qr, n)
	for i := 0; i < n; i++ {
		s := start + int64(i)*step
		e := s + step
		if i == n-1 {
			e = end
		} else {
			e += 1 // 段边界重叠 1ns：边界点两段都查，归并去重
		}
		go func(idx int, s, e int64) {
			pts, err := p.client.QueryRange(ctx, s, e, p.queryOptions())
			results <- qr{idx, pts, err}
		}(i, s, e)
	}
	// 收集全部结果后按段序（idx）归并，保证输出时间升序；
	// 边界去重直接在归并时完成（只处理边界 ts 的少量点）
	resps := make([]qr, 0, n)
	for i := 0; i < n; i++ {
		resps = append(resps, <-results)
	}
	sort.Slice(resps, func(i, j int) bool { return resps[i].idx < resps[j].idx })
	all := make([]model.Point, 0, (end-start)/1e6)
	var firstErr error
	for i, r := range resps {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		pts := r.points
		if i > 0 {
			// 边界去重：前段尾部与当前段头部 ts==boundary 的点可能重复
			// （若 i-1 段查询失败未贡献点，all 尾部无 boundary 点则不触发）
			boundary := start + int64(i)*step
			t := len(all)
			for t > 0 && all[t-1].Timestamp == boundary {
				t--
			}
			h := 0
			for h < len(pts) && pts[h].Timestamp == boundary {
				h++
			}
			if t < len(all) && h > 0 {
				var set boundarySet
				for j := t; j < len(all); j++ {
					set.add(all[j])
				}
				prevTail := all[t:] // 前段尾部边界点（权威副本，保留）
				all = all[:t]
				all = append(all, prevTail...)
				for j := 0; j < h; j++ {
					if !set.contains(pts[j]) {
						all = append(all, pts[j])
					}
				}
				all = append(all, pts[h:]...)
				continue
			}
		}
		all = append(all, pts...)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return all, nil
}

// boundarySet 边界时间戳去重集合：小集合用零分配的 PointsEqual 线性比较，
// 大集合自动切换 Key 映射（大集合才付出 Key 字符串构造成本）。
type boundarySet struct {
	small []model.Point
	big   map[string]struct{}
}

const boundaryLinearLimit = 8

func (s *boundarySet) add(p model.Point) {
	if len(s.small) < boundaryLinearLimit {
		s.small = append(s.small, p)
		return
	}
	if s.big == nil {
		s.big = make(map[string]struct{}, boundaryLinearLimit*4)
		for _, q := range s.small {
			s.big[q.Key()] = struct{}{}
		}
		s.small = nil
	}
	s.big[p.Key()] = struct{}{}
}

func (s *boundarySet) contains(p model.Point) bool {
	for _, q := range s.small {
		if model.PointsEqual(q, p) {
			return true
		}
	}
	if s.big != nil {
		_, ok := s.big[p.Key()]
		return ok
	}
	return false
}

// effectiveBatchPoints 降速时批次减半（黄色反压）。
func (p *Poller) effectiveBatchPoints() int {
	if p.metrics.BackpressureStatus() == int64(bpDegraded) {
		return p.cfg.BatchPoints / 2
	}
	return p.cfg.BatchPoints
}

// appendFrames 将窗口数据按 BatchPoints 分帧写入 WAL。
// 组帧（LinesToProtocol + gzip 编码）在 worker 池并行执行，
// 帧字节按 seq 顺序落盘（顺序铁律不变）。
// 单帧编码失败（压缩后超 1MB / 原始超 16MB）时按段减半拆批重试，
// 避免大帧永久失败导致游标卡死。
func (p *Poller) appendFrames(points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	batch := p.effectiveBatchPoints()
	if batch <= 0 {
		batch = p.cfg.BatchPoints
	}
	return p.appendBatch(points, batch)
}

// appendBatch 将 points 按 batch 分帧追加；编码失败且 batch>1 时对失败段减半重试。
// 组帧用 LinesToProtocolBytes（零行级分配）+ 并行编码；落盘用 AppendBatch
// group commit（每轮一次 fsync，替代每帧 fsync）。
// C4：batch=1 且单点仍超限时跳过该点并告警计数（防止游标永久卡死）。
func (p *Poller) appendBatch(points []model.Point, batch int) error {
	if len(points) == 0 {
		return nil
	}
	if batch < 1 {
		batch = 1
	}
	nFrames := (len(points) + batch - 1) / batch

	type framed struct {
		idx  int
		data []byte
		err  error
	}
	frames := make(chan framed, nFrames)
	workers := p.cfg.ParallelQueries
	if workers <= 1 {
		workers = 1
	}
	if workers > nFrames {
		workers = nFrames
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < nFrames; i += workers {
				start := i * batch
				endPt := start + batch
				if endPt > len(points) {
					endPt = len(points)
				}
				payload, err := model.LinesToProtocolBytes(points[start:endPt])
				if err != nil {
					frames <- framed{idx: i, err: err}
					continue
				}
				// seq 占位 0：真实 seq 由 WAL.AppendBatch 锁内分配（并发追加安全）
				typ := p.cfg.Compression
				if typ == 0 {
					typ = protocol.TypeDataZstd // 直接构造（测试）时的兜底默认
				}
				fb, err := protocol.Encode(typ, 0, payload)
				if err != nil {
					frames <- framed{idx: i, err: err}
					continue
				}
				frames <- framed{idx: i, data: fb}
			}
		}(w)
	}
	go func() { wg.Wait(); close(frames) }()

	// 按 idx 顺序收集
	got := make([]*framed, nFrames)
	for f := range frames {
		ff := f
		got[f.idx] = &ff
	}
	// 顺序处理（顺序铁律：seq 必须与 NextSeq 严格递增一致）
	var good [][]byte
	for i := 0; i < nFrames; i++ {
		f := got[i]
		if f == nil {
			return fmt.Errorf("frame %d missing", i)
		}
		if f.err != nil {
			if isFrameTooLarge(f.err) && batch > 1 {
				// 先落盘 i 之前的帧，再递归拆批失败段
				if len(good) > 0 {
					if err := p.appendGood(good); err != nil {
						return err
					}
					good = nil
				}
				start := i * batch
				endPt := start + batch
				if endPt > len(points) {
					endPt = len(points)
				}
				p.logger.Warn("frame too large, splitting batch",
					zap.Int("batch", batch), zap.Int("points", endPt-start))
				if err := p.appendBatch(points[start:endPt], batch/2); err != nil {
					return err
				}
				continue
			}
			if isFrameTooLarge(f.err) {
				// batch==1 且单点超 16MB/1MB 上限：跳过该点并告警计数，游标继续推进
				p.metrics.IncSkipPoint()
				p.logger.Error("point too large, skipped (would deadlock cursor)",
					zap.Int64("ts", points[i].Timestamp), zap.Error(f.err))
				continue
			}
			return f.err
		}
		good = append(good, f.data)
	}
	if len(good) > 0 {
		return p.appendGood(good)
	}
	return nil
}

// appendGood 落盘连续帧段（group commit）：seq 由 WAL.AppendBatch 内部分配（并发追加安全）。
func (p *Poller) appendGood(good [][]byte) error {
	_, err := p.wal.AppendBatch(protocol.TypeData, good)
	return err
}

// isFrameTooLarge 判断编码失败是否因帧超限（可拆批恢复）。
func isFrameTooLarge(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, protocol.ErrTooLarge) ||
		strings.Contains(err.Error(), "payload too large") ||
		strings.Contains(err.Error(), "frame too large")
}
