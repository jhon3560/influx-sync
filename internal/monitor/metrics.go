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

// --- 文档指标（《死信隔离与反压机制逻辑》）---

// SetWALDiskRatio 设置 WAL 挂载盘占用率（0~1，×10000 存储避免浮点原子）。
func (m *Metrics) SetWALDiskRatio(ratio float64) { m.walDiskRatio.Store(int64(ratio * 10000)) }

// SetBackpressureStatus 设置反压状态：0 正常 / 1 降速 / 2 挂起。
func (m *Metrics) SetBackpressureStatus(v int64) { m.bpStatus.Store(v) }

// AddPausedSeconds 累加 Poller 挂起秒数。
func (m *Metrics) AddPausedSeconds(v int64) { m.pausedSecs.Add(v) }

// IncPoisonPacket 累加毒丸报文计数。
func (m *Metrics) IncPoisonPacket() { m.poisonPacket.Add(1) }

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
# HELP dup_total Receiver 去重命中总数
# TYPE dup_total counter
dup_total %d
`,
		m.cursor.Load(), syncDelay,
		m.walPending.Load(), m.walBytes.Load(),
		m.sendTotal.Load(), m.ackOk.Load(), m.ackFail.Load(),
		m.retry.Load(), m.dlqTotal.Load(), m.heartbeat.Load(), m.pollSkip.Load(),
		m.recvTotal.Load(), m.writeOk.Load(), m.writeFail.Load(), m.dupTotal.Load())
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
func (m *Metrics) NewHTTPServer(addr string, auth *Auth) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.authMiddleware(auth, m.Handler()))
	return &http.Server{Addr: addr, Handler: mux}
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
