// Package monitor 提供 Prometheus 文本格式的运行时指标。
package monitor

import (
	"crypto/hmac"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// Metrics 指标集合（线程安全，全 atomic）。
type Metrics struct {
	cursor     atomic.Int64  // sync_cursor：当前逻辑游标（ns）
	walPending atomic.Int64  // wal_pending：未确认帧数
	walBytes   atomic.Int64  // wal_size_bytes
	sendTotal  atomic.Uint64 // send_total：发送帧数
	ackOk      atomic.Uint64 // ack_success
	ackFail    atomic.Uint64 // ack_fail
	retry      atomic.Uint64 // retry_total
	dlqTotal   atomic.Uint64 // dlq_total
	heartbeat  atomic.Uint64 // heartbeat_total
	pollSkip   atomic.Uint64 // poll_skip：反压跳过的轮询
	writeFail  atomic.Uint64 // receiver write_fail
	writeOk    atomic.Uint64 // receiver write_ok
	recvTotal  atomic.Uint64 // receiver recv_total
	dupTotal   atomic.Uint64 // receiver dup_total
	// 文档指标（《死信隔离与反压机制逻辑》）
	walDiskRatio atomic.Int64  // influx_sync_wal_disk_usage_ratio（×10000 存储）
	bpStatus     atomic.Int64  // influx_sync_backpressure_status 0/1/2
	pausedSecs   atomic.Int64  // influx_sync_poller_paused_seconds_total
	poisonPacket atomic.Uint64 // influx_sync_poison_packet_count
	// V1.4 新增
	skipPoint   atomic.Uint64 // point_skip_total：超限跳过的单点（防游标卡死）
	lastWriteTs atomic.Int64  // receiver 最后落库点时间戳（ns，0=未知）
	relayDLQ    atomic.Uint64 // relay_dlq_total：中继 WAL 失败转存计数
	inflight    atomic.Int64  // receiver recv_inflight：在途处理帧数
	// V1.5 A4 fast-path 新增
	fpState    atomic.Int64  // fast_path_state: 0=off/1=waiting/2=active
	fpBatch    atomic.Uint64 // fast_path_batches_total
	fpPoints   atomic.Uint64 // fast_path_points_total
	fpSigOnly  atomic.Uint64 // fast_path_signal_only_total（WAITING 期仅信号）
	fpDropBig  atomic.Uint64 // fast_path_dropped_oversize_total
	fpDropPrec atomic.Uint64 // fast_path_dropped_precision_total
	fpDropBP   atomic.Uint64 // fast_path_dropped_backpressure_total
	fpLineSkip atomic.Uint64 // fast_path_line_skipped_total
	fpDedupHit atomic.Uint64 // fast_path_dedup_hits_total（Poller 侧命中）
}

// New 创建指标集合。
func New() *Metrics { return &Metrics{} }

// --- 指标更新 ---

func (m *Metrics) SetCursor(v int64)     { m.cursor.Store(v) }
func (m *Metrics) SetWALPending(v int64) { m.walPending.Store(v) }
func (m *Metrics) SetWALBytes(v int64)   { m.walBytes.Store(v) }
func (m *Metrics) IncSend()              { m.sendTotal.Add(1) }
func (m *Metrics) IncAckOk()             { m.ackOk.Add(1) }
func (m *Metrics) IncAckFail()           { m.ackFail.Add(1) }
func (m *Metrics) IncRetry()             { m.retry.Add(1) }
func (m *Metrics) IncDLQ()               { m.dlqTotal.Add(1) }
func (m *Metrics) IncHeartbeat()         { m.heartbeat.Add(1) }
func (m *Metrics) IncPollSkip()          { m.pollSkip.Add(1) }
func (m *Metrics) IncWriteFail()         { m.writeFail.Add(1) }
func (m *Metrics) IncWriteOk()           { m.writeOk.Add(1) }
func (m *Metrics) IncRecv()              { m.recvTotal.Add(1) }
func (m *Metrics) IncDup()               { m.dupTotal.Add(1) }

// AckFailCount 返回 ACK 失败计数（测试用）。
func (m *Metrics) AckFailCount() uint64 { return m.ackFail.Load() }

// AckOkCount 返回 ACK 成功计数（测试用）。
func (m *Metrics) AckOkCount() uint64 { return m.ackOk.Load() }

// --- 文档指标（《死信隔离与反压机制逻辑》）---

// SetWALDiskRatio 设置 WAL 挂载盘占用率（0~1，×10000 存储避免浮点原子）。
func (m *Metrics) SetWALDiskRatio(ratio float64) { m.walDiskRatio.Store(int64(ratio * 10000)) }

// SetBackpressureStatus 设置反压状态：0 正常 / 1 降速 / 2 挂起。
func (m *Metrics) SetBackpressureStatus(v int64) { m.bpStatus.Store(v) }

// AddPausedSeconds 累加 Poller 挂起秒数。
func (m *Metrics) AddPausedSeconds(v int64) { m.pausedSecs.Add(v) }

// IncPoisonPacket 累加毒丸报文计数。
func (m *Metrics) IncPoisonPacket() { m.poisonPacket.Add(1) }

// IncSkipPoint 累加超限跳过的单点计数（batch=1 仍超 16MB/1MB 的病理点）。
func (m *Metrics) IncSkipPoint() { m.skipPoint.Add(1) }

// SkipPointCount 返回跳点计数（测试用）。
func (m *Metrics) SkipPointCount() uint64 { return m.skipPoint.Load() }

// SetLastWriteTs 记录 receiver 最后落库点时间戳（只增不减）。
func (m *Metrics) SetLastWriteTs(ts int64) {
	for {
		cur := m.lastWriteTs.Load()
		if ts <= cur || m.lastWriteTs.CompareAndSwap(cur, ts) {
			return
		}
	}
}

// LastWriteTs 返回最后落库点时间戳（测试用）。
func (m *Metrics) LastWriteTs() int64 { return m.lastWriteTs.Load() }

// IncRelayDLQ 累加中继转发失败转存计数。
func (m *Metrics) IncRelayDLQ() { m.relayDLQ.Add(1) }

// RelayDLQCount 返回中继转存计数（测试用）。
func (m *Metrics) RelayDLQCount() uint64 { return m.relayDLQ.Load() }

// --- V1.5 A4 fast-path 指标 ---

func (m *Metrics) SetFastPathState(v int64)        { m.fpState.Store(v) }
func (m *Metrics) IncFastPathBatch()               { m.fpBatch.Add(1) }
func (m *Metrics) AddFastPathPoints(v int64)       { m.fpPoints.Add(uint64(v)) }
func (m *Metrics) IncFastPathSignalOnly()          { m.fpSigOnly.Add(1) }
func (m *Metrics) IncFastPathDroppedOversize()     { m.fpDropBig.Add(1) }
func (m *Metrics) IncFastPathDroppedPrecision()    { m.fpDropPrec.Add(1) }
func (m *Metrics) IncFastPathDroppedBackpressure() { m.fpDropBP.Add(1) }
func (m *Metrics) IncFastPathLineSkipped()         { m.fpLineSkip.Add(1) }
func (m *Metrics) IncFastPathDedupHit()            { m.fpDedupHit.Add(1) }

// FastPathState 返回快路径状态（测试用）。
func (m *Metrics) FastPathState() int64 { return m.fpState.Load() }

// FastPathBatches 返回批计数（测试用）。
func (m *Metrics) FastPathBatches() uint64 { return m.fpBatch.Load() }

// FastPathPoints 返回点数（测试用）。
func (m *Metrics) FastPathPoints() uint64 { return m.fpPoints.Load() }

// FastPathSignalOnly 返回仅信号批计数（测试用）。
func (m *Metrics) FastPathSignalOnly() uint64 { return m.fpSigOnly.Load() }

// FastPathDroppedOversize 返回超限丢批计数（测试用）。
func (m *Metrics) FastPathDroppedOversize() uint64 { return m.fpDropBig.Load() }

// FastPathDroppedPrecision 返回精度丢批计数（测试用）。
func (m *Metrics) FastPathDroppedPrecision() uint64 { return m.fpDropPrec.Load() }

// FastPathDroppedBackpressure 返回反压丢批计数（测试用）。
func (m *Metrics) FastPathDroppedBackpressure() uint64 { return m.fpDropBP.Load() }

// FastPathLineSkipped 返回行跳过计数（测试用）。
func (m *Metrics) FastPathLineSkipped() uint64 { return m.fpLineSkip.Load() }

// FastPathDedupHit 返回去重命中计数（测试用）。
func (m *Metrics) FastPathDedupHit() uint64 { return m.fpDedupHit.Load() }

// IncInflight / DecInflight 维护 receiver 在途帧数（N2 小项：替代废弃 LRU 的可观测性）。
func (m *Metrics) IncInflight() { m.inflight.Add(1) }
func (m *Metrics) DecInflight() { m.inflight.Add(-1) }

// Inflight 返回当前在途帧数（测试用）。
func (m *Metrics) Inflight() int64 { return m.inflight.Load() }

// DLQCount 返回死信计数（测试用）。
func (m *Metrics) DLQCount() uint64 { return m.dlqTotal.Load() }

// PoisonCount 返回毒丸计数（测试用）。
func (m *Metrics) PoisonCount() uint64 { return m.poisonPacket.Load() }

// BackpressureStatus 返回反压状态（测试用）。
func (m *Metrics) BackpressureStatus() int64 { return m.bpStatus.Load() }

// Render 输出 Prometheus 文本格式。
func (m *Metrics) Render() []byte {
	now := nowUnixNano()
	syncDelay := (now - m.cursor.Load()) / 1e9
	if syncDelay < 0 {
		syncDelay = 0
	}
	out := fmt.Sprintf(`# HELP sync_cursor_ns 当前逻辑游标（已进入 WAL 的最大数据时间）
# TYPE sync_cursor_ns gauge
sync_cursor_ns %d
# HELP sync_delay_seconds 同步延迟（now - cursor）
# TYPE sync_delay_seconds gauge
sync_delay_seconds %d
# HELP wal_pending 未确认帧数
# TYPE wal_pending gauge
wal_pending %d
# HELP wal_size_bytes WAL 目录占用
# TYPE wal_size_bytes gauge
wal_size_bytes %d
# HELP send_total 发送帧总数
# TYPE send_total counter
send_total %d
# HELP ack_success ACK 成功总数
# TYPE ack_success counter
ack_success %d
# HELP ack_fail ACK 失败总数
# TYPE ack_fail counter
ack_fail %d
# HELP retry_total 重试总数
# TYPE retry_total counter
retry_total %d
# HELP dlq_total 死信转存总数
# TYPE dlq_total counter
dlq_total %d
# HELP heartbeat_total 心跳总数
# TYPE heartbeat_total counter
heartbeat_total %d
# HELP poll_skip 反压跳过轮询总数
# TYPE poll_skip counter
poll_skip %d
# HELP recv_total Receiver 收到帧总数
# TYPE recv_total counter
recv_total %d
# HELP write_ok Receiver 写库成功总数
# TYPE write_ok counter
write_ok %d
# HELP write_fail Receiver 写库失败总数
# TYPE write_fail counter
write_fail %d
# HELP recv_inflight Receiver 在途处理帧数
# TYPE recv_inflight gauge
recv_inflight %d
# HELP dup_total Receiver 去重命中总数
# TYPE dup_total counter
dup_total %d
# HELP point_skip_total 超限跳过单点总数（batch=1 仍超上限的病理点）
# TYPE point_skip_total counter
point_skip_total %d
# HELP relay_dlq_total 中继转发失败转存总数
# TYPE relay_dlq_total counter
relay_dlq_total %d
# HELP sync_e2e_delay_seconds 端到端延迟（now - 目标库最后写入点时间，0=未知）
# TYPE sync_e2e_delay_seconds gauge
sync_e2e_delay_seconds %d
# HELP fast_path_state 快路径状态 0=off 2=active(透传)（V1.7 起无 waiting 态）
# TYPE fast_path_state gauge
fast_path_state %d
# HELP fast_path_batches_total 快路径收到推送批总数
# TYPE fast_path_batches_total counter
fast_path_batches_total %d
# HELP fast_path_points_total 快路径转发点总数
# TYPE fast_path_points_total counter
fast_path_points_total %d
# HELP fast_path_signal_only_total WAITING 期仅作信号的批数
# TYPE fast_path_signal_only_total counter
fast_path_signal_only_total %d
# HELP fast_path_dropped_oversize_total 超限跳过批数（Poller 兜底）
# TYPE fast_path_dropped_oversize_total counter
fast_path_dropped_oversize_total %d
# HELP fast_path_dropped_precision_total 非 ns 精度跳过批数（Poller 兜底）
# TYPE fast_path_dropped_precision_total counter
fast_path_dropped_precision_total %d
# HELP fast_path_dropped_backpressure_total 反压跳过批数（Poller 兜底）
# TYPE fast_path_dropped_backpressure_total counter
fast_path_dropped_backpressure_total %d
# HELP fast_path_line_skipped_total 逐行跳过行数（坏行/非 ns/非目标 measurement）
# TYPE fast_path_line_skipped_total counter
fast_path_line_skipped_total %d
# HELP fast_path_dedup_hits_total Poller 侧去重命中点数（跳过已由快路径转发的点）
# TYPE fast_path_dedup_hits_total counter
fast_path_dedup_hits_total %d
`,
		m.cursor.Load(), syncDelay,
		m.walPending.Load(), m.walBytes.Load(),
		m.sendTotal.Load(), m.ackOk.Load(), m.ackFail.Load(),
		m.retry.Load(), m.dlqTotal.Load(), m.heartbeat.Load(), m.pollSkip.Load(),
		m.recvTotal.Load(), m.writeOk.Load(), m.writeFail.Load(),
		m.inflight.Load(), m.dupTotal.Load(),
		m.skipPoint.Load(), m.relayDLQ.Load(), m.e2eDelay(),
		m.fpState.Load(), m.fpBatch.Load(), m.fpPoints.Load(), m.fpSigOnly.Load(),
		m.fpDropBig.Load(), m.fpDropPrec.Load(), m.fpDropBP.Load(),
		m.fpLineSkip.Load(), m.fpDedupHit.Load())
	// 文档指标（《死信隔离与反压机制逻辑》）
	out += fmt.Sprintf(`# HELP influx_sync_wal_disk_usage_ratio WAL 挂载盘占用率
# TYPE influx_sync_wal_disk_usage_ratio gauge
influx_sync_wal_disk_usage_ratio %.4f
# HELP influx_sync_backpressure_status 反压状态 0=正常 1=降速 2=挂起
# TYPE influx_sync_backpressure_status gauge
influx_sync_backpressure_status %d
# HELP influx_sync_poller_paused_seconds_total Poller 反压挂起累计秒数
# TYPE influx_sync_poller_paused_seconds_total counter
influx_sync_poller_paused_seconds_total %d
# HELP influx_sync_dlq_generated_total 死信隔离总帧数
# TYPE influx_sync_dlq_generated_total counter
influx_sync_dlq_generated_total %d
# HELP influx_sync_poison_packet_count 毒丸报文数
# TYPE influx_sync_poison_packet_count counter
influx_sync_poison_packet_count %d
`, float64(m.walDiskRatio.Load())/10000, m.bpStatus.Load(), m.pausedSecs.Load(), m.dlqTotal.Load(), m.poisonPacket.Load())
	return []byte(out)
}

// nowUnixNano 可被测试替换。
var nowUnixNano = func() int64 { return time.Now().UnixNano() }

// e2eDelay 端到端延迟秒数（now - 最后落库点时间）。0=未知（尚无写入）。
func (m *Metrics) e2eDelay() int64 {
	ts := m.lastWriteTs.Load()
	if ts == 0 {
		return 0
	}
	d := (nowUnixNano() - ts) / 1e9
	if d < 0 {
		d = 0
	}
	return d
}

// Auth 监控端口认证配置（nil/空用户名=不启用认证）。
type Auth struct {
	Username string
	Password string
}

// Handler 返回 /metrics HTTP Handler。
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write(m.Render())
	})
}

// NewHTTPServer 创建指标 HTTP 服务（由调用方启动/关闭）。
// auth 为 nil 或用户名为空时不启用认证（兼容旧部署）。
// 加 ReadTimeout/ReadHeaderTimeout：防止慢连接占住指标端口（小项加固）。
func (m *Metrics) NewHTTPServer(addr string, auth *Auth) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.authMiddleware(auth, m.Handler()))
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// authMiddleware 实现 HTTP Basic Auth；密码比较使用常量时间算法（防定时攻击）。
func (m *Metrics) authMiddleware(auth *Auth, next http.Handler) http.Handler {
	if auth == nil || auth.Username == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			!hmac.Equal([]byte(u), []byte(auth.Username)) ||
			!hmac.Equal([]byte(p), []byte(auth.Password)) {
			w.Header().Set("WWW-Authenticate", `Basic realm="influx-sync metrics"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
