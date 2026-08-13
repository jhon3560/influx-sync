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
// V1.2 支持订阅信号触发：源库 SUBSCRIPTION 推送到达时立即查询（信号合并，
// 不解析内容；丢失时由兜底 ticker 保证不漏）。
type Poller struct {
	client  *influx.Client
	wal     *wal.WAL
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     PollerConfig

	signalCh        chan struct{} // 订阅信号（cap=1，合并语义：忙时丢弃新信号）
	lastPoll        time.Time     // 上次查询时间（最小信号间隔护栏）
	wakeupScheduled atomic.Bool   // watermark 解锁自唤醒定时器是否已安排（原子，防竞态）
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
	return &Poller{client: client, wal: w, metrics: metrics, logger: logger, cfg: cfg, signalCh: make(chan struct{}, 1)}
}

// Notify 订阅信号回调：非阻塞发信号（cap=1 天然合并，Poller 忙时丢弃）。
func (p *Poller) Notify() {
	select {
	case p.signalCh <- struct{}{}:
	default:
		// 已有待处理信号或 Poller 忙：丢弃（合并），不影响正确性
	}
}

// Run 阻塞运行，直到 ctx 取消。
// 驱动源：订阅信号（优先，事件驱动）+ 兜底 ticker（信号丢失/订阅失效时保底）。
func (p *Poller) Run(ctx context.Context) {
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
			// 订阅信号触发：最小间隔护栏，防高频信号打满 Poller
			if time.Since(p.lastPoll) >= p.cfg.MinSignalInterval {
				triggered = true
			}
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
		p.lastPoll = time.Now()
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
	var st syscall.Statfs_t
	if err := syscall.Statfs(p.wal.Dir(), &st); err != nil {
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
	end := cursor + int64(p.cfg.Window)
	if maxEnd := now - int64(p.cfg.Watermark); end > maxEnd {
		end = maxEnd
		blocked = true
	}
	// 窗口上限保护：即使时间跳变/积压，单次最多查 MaxWindow
	if end-cursor > int64(p.cfg.MaxWindow) {
		end = cursor + int64(p.cfg.MaxWindow)
	}
	if end <= cursor {
		return false // 无新窗口
	}

	points, err := p.queryParallel(ctx, cursor, end)
	if err != nil {
		p.logger.Warn("query failed, keep cursor", zap.Error(err))
		return false // 保持游标，下轮重试
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

// queryOptions 组装查询选项。
func (p *Poller) queryOptions() influx.QueryOptions {
	return influx.QueryOptions{
		Measurements: p.cfg.Measurements,
		Limit:        p.cfg.QueryLimit,
		TagColumns:   p.cfg.TagColumns,
	}
}

// queryParallel 将 [start,end) 窗口切分为 N 段并行查询，结果按段序归并。
// 段边界 +1ns 重叠（V1.2.3）：边界点由相邻两段重复查询，归并时按
// Point.Key()（measurement+tags+timestamp）去重——防御 InfluxDB 高压写入时
// 并行查询偶发漏行（实测 11:33:01.71884Z 丢 1 点）。重复查询的点由
// InfluxDB 幂等覆盖保证不产生重复计数。
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
	// 收集全部结果后按段序（idx）归并，保证输出时间升序
	resps := make([]qr, 0, n)
	for i := 0; i < n; i++ {
		resps = append(resps, <-results)
	}
	sort.Slice(resps, func(i, j int) bool { return resps[i].idx < resps[j].idx })
	all := make([]model.Point, 0, (end-start)/1e6)
	seen := make(map[string]struct{}, (end-start)/1e6)
	var firstErr error
	for _, r := range resps {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		for _, pt := range r.points {
			k := pt.Key()
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			all = append(all, pt)
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return all, nil
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
func (p *Poller) appendBatch(points []model.Point, batch int) error {
	if len(points) == 0 {
		return nil
	}
	if batch < 1 {
		batch = 1
	}
	nFrames := (len(points) + batch - 1) / batch
	baseSeq := p.wal.NextSeq()

	type framed struct {
		idx  int
		seq  uint64
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
				lines, err := model.LinesToProtocol(points[start:endPt])
				if err != nil {
					frames <- framed{idx: i, err: err}
					continue
				}
				payload := []byte(strings.Join(lines, "\n"))
				fb, err := protocol.Encode(protocol.TypeData, baseSeq+uint64(i), payload)
				if err != nil {
					frames <- framed{idx: i, err: err}
					continue
				}
				frames <- framed{idx: i, seq: baseSeq + uint64(i), data: fb}
			}
		}(w)
	}
	go func() { wg.Wait(); close(frames) }()

	// 按 idx 顺序落 WAL（顺序铁律：seq 必须与 NextSeq 严格递增一致）
	got := make([]*framed, nFrames)
	for f := range frames {
		ff := f
		got[f.idx] = &ff
	}
	for i := 0; i < nFrames; i++ {
		f := got[i]
		if f == nil {
			return fmt.Errorf("frame %d missing", i)
		}
		if f.err != nil {
			if batch > 1 && isFrameTooLarge(f.err) {
				start := i * batch
				endPt := start + batch
				if endPt > len(points) {
					endPt = len(points)
				}
				p.logger.Warn("frame too large, splitting batch",
					zap.Int("batch", batch), zap.Int("points", endPt-start))
				// 注意：i 之前的帧已落盘，递归只重试失败段（seq 从 NextSeq 继续）
				return p.appendBatch(points[start:endPt], batch/2)
			}
			return f.err
		}
		if err := p.wal.AppendEncoded(protocol.TypeData, f.seq, f.data); err != nil {
			return err
		}
	}
	return nil
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
