// Package sender 实现 I 区同步发送端：轮询查询 → WAL → 停等发送。
package sender

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	wakeupScheduled bool          // watermark 解锁自唤醒定时器是否已安排（防重复）
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
			// 黄色降速：间隔拉长至 5s，Batch 减半
			time.Sleep(5 * time.Second)
		}

		blocked := p.pollOnce(ctx)
		p.lastPoll = time.Now()
		// watermark 解锁自唤醒（仅信号模式）：本轮窗口被水位截断时，
		// 安排 watermark 时长后自动再查一次，避免等待下一个 ticker 的闲置延迟
		if blocked && !p.wakeupScheduled {
			p.wakeupScheduled = true
			go func() {
				time.Sleep(p.cfg.Watermark)
				p.wakeupScheduled = false
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
// 并行数<=1 时退化为单次串行查询（行为与 V1.0 完全一致）。
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
		}
		go func(idx int, s, e int64) {
			pts, err := p.client.QueryRange(ctx, s, e, p.queryOptions())
			results <- qr{idx, pts, err}
		}(i, s, e)
	}
	all := make([]model.Point, 0, (end-start)/1e6)
	var firstErr error
	for i := 0; i < n; i++ {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		all = append(all, r.points...)
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
func (p *Poller) appendFrames(points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	batch := p.effectiveBatchPoints()
	if batch <= 0 {
		batch = p.cfg.BatchPoints
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
			return f.err
		}
		if err := p.wal.AppendEncoded(protocol.TypeData, f.seq, f.data); err != nil {
			return err
		}
	}
	return nil
}
