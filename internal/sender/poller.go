// Package sender 实现 I 区同步发送端：轮询查询 → WAL → 停等发送。
package sender

import (
	"context"
	"fmt"
	"strings"
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
	Interval     time.Duration // 轮询周期，默认 1s
	Window       time.Duration // 查询窗口，默认 5s
	Watermark    time.Duration // 水位延迟，默认 10s
	MaxWindow    time.Duration // 单次查询窗口上限（防时间跳变），默认 30s
	BatchPoints  int           // 单帧点数，默认 10000
	QueryLimit   int           // 单次查询 LIMIT（越大分页越少，扫描开销越低），默认 100000
	TagColumns   []string      // 显式 tag 列（空=自动发现）
	Measurements []string      // 要同步的 measurement 列表（空=全部）
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
type Poller struct {
	client  *influx.Client
	wal     *wal.WAL
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     PollerConfig
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
	return &Poller{client: client, wal: w, metrics: metrics, logger: logger, cfg: cfg}
}

// Run 阻塞运行，直到 ctx 取消。
func (p *Poller) Run(ctx context.Context) {
	state := bpNormal
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	pausedStart := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
		p.pollOnce(ctx)
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

// pollOnce 执行一轮查询。
func (p *Poller) pollOnce(ctx context.Context) {
	now := time.Now().UnixNano()
	cursor := p.wal.Cursor()
	end := cursor + int64(p.cfg.Window)
	if maxEnd := now - int64(p.cfg.Watermark); end > maxEnd {
		end = maxEnd
	}
	// 窗口上限保护：即使时间跳变/积压，单次最多查 MaxWindow
	if end-cursor > int64(p.cfg.MaxWindow) {
		end = cursor + int64(p.cfg.MaxWindow)
	}
	if end <= cursor {
		return // 无新窗口
	}

	points, err := p.client.QueryRange(ctx, cursor, end, influx.QueryOptions{
		Measurements: p.cfg.Measurements,
		Limit:        p.cfg.QueryLimit,
		TagColumns:   p.cfg.TagColumns,
	})
	if err != nil {
		p.logger.Warn("query failed, keep cursor", zap.Error(err))
		return // 保持游标，下轮重试
	}
	p.logger.Info("poll window", zap.Int64("start", cursor), zap.Int64("end", end), zap.Int("points", len(points)))

	// 先写 WAL，成功后才推进游标（顺序铁律，违反会漏数据）
	if err := p.appendFrames(points); err != nil {
		p.logger.Error("wal append failed, keep cursor", zap.Error(err))
		return
	}
	if err := p.wal.SetCursor(end); err != nil {
		p.logger.Error("cursor update failed", zap.Error(err))
		return
	}
	p.metrics.SetCursor(end)
	// 每轮刷新 WAL 状态指标（断连积压时可观测）
	p.metrics.SetWALPending(int64(p.wal.PendingCount()))
	p.metrics.SetWALBytes(p.wal.DiskUsage())
}

// effectiveBatchPoints 降速时批次减半（黄色反压）。
func (p *Poller) effectiveBatchPoints() int {
	if p.metrics.BackpressureStatus() == int64(bpDegraded) {
		return p.cfg.BatchPoints / 2
	}
	return p.cfg.BatchPoints
}

// appendFrames 将窗口数据按 BatchPoints 分帧写入 WAL。
func (p *Poller) appendFrames(points []model.Point) error {
	if len(points) == 0 {
		return nil
	}
	batch := p.effectiveBatchPoints()
	if batch <= 0 {
		batch = p.cfg.BatchPoints
	}
	for start := 0; start < len(points); start += batch {
		end := start + batch
		if end > len(points) {
			end = len(points)
		}
		lines, err := model.LinesToProtocol(points[start:end])
		if err != nil {
			return fmt.Errorf("convert points: %w", err)
		}
		payload := []byte(strings.Join(lines, "\n"))
		if _, err := p.wal.Append(protocol.TypeData, payload); err != nil {
			return err
		}
	}
	return nil
}
