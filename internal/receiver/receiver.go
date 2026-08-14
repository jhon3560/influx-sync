package receiver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/influx"
	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/wal"
)

// Config Receiver 配置。
type Config struct {
	LastSeqFile string      // last_seq 持久化路径（空=不持久化）
	DLQDir      string      // 毒丸死信目录（空=禁用 DLQ）
	RelayWAL    *wal.WAL    // 中继转发 WAL（V1.3；空=不启用中继）
	RelayDLQDir string      // 中继转发失败转存目录（C2：空=退化为仅告警）
	LastWriteTs func(int64) // 可选：落库点时间戳回调（A5 e2e 延迟指标），nil=不回调
}

// seqJumpLimit 允许的最大 seq 跳跃（防外部/异常帧污染 last_seq）。
const seqJumpLimit uint64 = 100000

// Receiver 帧处理器：Decode → 去重 → 写 Influx → ACK。
// 依据协议：写库成功后才回 0xff，保证“ACK = 已落库”。
type Receiver struct {
	client  *influx.Client
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     Config
	lastSeq atomic.Int64 // 已成功处理的最大连续 seq（内存；只增不减）
	seqOrd  *seqTracker  // 按序 seq 推进器（N2：恒开——流水线/多连接下乱序完成安全）
	// N6：永久缺口闭合（发送方权威）。frameIdx 由 tcp_server 读帧循环按序分配
	// （数据帧计数、心跳不计入）：frameIdx==0 即"新连接首帧"= sender WAL 头。
	// 若 seq > lastSeq+1，说明 [lastSeq+1, seq-1] 已在 sender 侧 Commit
	// （"0xff=已落库"保证它们完成于旧进程），可安全 markDone 闭合。
	// 双保险：缺口区间必须全部不在途（防多 sender 误配场景）。
	inflightMu  sync.Mutex
	inflightSeq map[uint64]struct{} // 当前在途帧 seq
	gapWarned   atomic.Uint64       // 上次 Warn 的缺口首帧 seq（日志节流）

	persistMu sync.Mutex
	persistAt time.Time // last_seq 持久化节流（每秒最多一次）
}

// New 创建 Receiver。
// last_seq 采用**连续前缀推进**语义（seqTracker 恒开，N2）：
// 乱序完成的帧只记入 pending，绝不把 last_seq 推过未完成的在途帧——
// 否则该帧瞬时失败后的重传会被 "seq<=last_seq" 吞掉（数据丢失）。
// 永久缺口（重启后 last_seq 文件节流落后）由新连接首帧闭合（N6）。
func New(client *influx.Client, metrics *monitor.Metrics, logger *zap.Logger, cfg Config) (*Receiver, error) {
	r := &Receiver{
		client:      client,
		metrics:     metrics,
		logger:      logger,
		cfg:         cfg,
		seqOrd:      newSeqTracker(),
		inflightSeq: make(map[uint64]struct{}),
	}
	if cfg.LastSeqFile != "" {
		seq, err := loadLastSeq(cfg.LastSeqFile)
		if err != nil {
			return nil, err
		}
		r.lastSeq.Store(seq)
		r.seqOrd.init(uint64(seq))
		logger.Info("receiver restored last_seq", zap.Int64("seq", seq))
	}
	return r, nil
}

// HandleFrame 处理一帧，返回 ACK 字节。线程安全（多连接/并发流水线可调用）。
// frameIdx：该连接内数据帧的到达序号（0 起，由 tcp_server 按序分配）——
// frameIdx==0 即新连接首帧（sender WAL 头），用于 N6 永久缺口闭合。
func (r *Receiver) HandleFrame(connID uint64, frameIdx uint64, frameBytes []byte) byte {
	r.metrics.IncRecv()
	f, err := protocol.Decode(frameBytes)
	if err != nil {
		r.logger.Warn("frame decode failed", zap.Uint64("conn", connID), zap.Error(err))
		return protocol.AckFail
	}

	// 心跳：确认通道活性，不写库
	if f.IsHeartbeat() {
		return protocol.AckSuccess
	}

	// 重复检测：已成功处理的连续前缀直接确认
	// （并发写库下 last_seq 只按连续前缀推进，保证此判定绝不吞掉未完成帧）
	if f.Seq <= uint64(r.lastSeq.Load()) {
		r.metrics.IncDup()
		r.logger.Debug("duplicate seq (<=last_seq)", zap.Uint64("seq", f.Seq))
		return protocol.AckSuccess
	}

	// 在途登记（N6 缺口闭合双保险 + recv_inflight 指标）
	r.metrics.IncInflight()
	defer r.metrics.DecInflight()
	r.addInflight(f.Seq)
	defer r.removeInflight(f.Seq)

	// seq 跳跃处理：接受但不拒绝（Influx 幂等覆盖保证最终一致，拒绝会导致
	// Sender 停等重发同一帧、链路永久卡死）。
	// N6：永久缺口（重启后 last_seq 文件节流落后）由新连接首帧闭合——
	// 避免 last_seq 冻结 + 逐帧 Warn 刷屏 + 去重失效。
	last := uint64(r.lastSeq.Load())
	if f.Seq > last+1 {
		if f.Seq > last+seqJumpLimit {
			// 大跳跃（双方持久化重置）：tracker 在 markDone 时直接越过；日志节流
			if r.gapWarned.CompareAndSwap(0, f.Seq) {
				r.logger.Error("seq jump too large, frame accepted anyway (idempotent overwrite)",
					zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()))
			} else {
				r.logger.Debug("seq jump (repeated)", zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()))
			}
		} else if frameIdx == 0 && r.tryCloseGap(f.Seq) {
			r.logger.Info("permanent seq gap closed via sender wal head",
				zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()), zap.Uint64("conn", connID))
		} else {
			// 无法安全闭合（在途冲突/非首帧）：日志节流，首条 Warn 后续 Debug
			if r.gapWarned.CompareAndSwap(0, f.Seq) {
				r.logger.Warn("seq jump", zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()))
			} else {
				r.logger.Debug("seq jump (repeated)", zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()))
			}
		}
	}

	raw, err := f.Decompress()
	if err != nil {
		r.logger.Warn("decompress failed", zap.Uint64("seq", f.Seq), zap.Error(err))
		return protocol.AckFail
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		r.logger.Warn("empty payload", zap.Uint64("seq", f.Seq))
		return protocol.AckFail
	}

	// 写库超时按批大小动态调整（基础 10s + 每行 1ms，封顶 120s）——
	// 固定 30s 在大 batch 高压写库时可能超时导致假失败重发。
	nLines := bytes.Count(raw, []byte("\n"))
	timeout := 10*time.Second + time.Duration(nLines)*time.Millisecond
	if timeout > 120*time.Second {
		timeout = 120 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// P5：解压出的 raw 本身就是合法 Line Protocol，直接整体写入
	// （省掉 splitLines→strings.Join 的拆拼往返与 1.85MB/帧分配）。
	if err := r.client.WriteRaw(ctx, raw); err != nil {
		r.metrics.IncWriteFail()
		permanent, httpStatus, category := classifyWriteError(err)
		if permanent {
			// 毒丸报文（HTTP 400 等永久错误）：剥离坏数据，保全主通道。
			// 落盘 DLQ 后回 0xff（欺骗性 ACK），Sender 删除 WAL 继续前进。
			r.metrics.IncPoisonPacket()
			meta := DLQMeta{
				SeqNum:        f.Seq,
				RetryAttempts: 1,
			}
			meta.ErrorContext.Category = category
			meta.ErrorContext.HTTPStatus = httpStatus
			meta.ErrorContext.ErrorMessage = err.Error()
			meta.DataMetadata.SourceZone = "Zone_I"
			lines := splitLines(raw) // 仅失败路径才拆行（元信息提取用）
			meta.DataMetadata.Measurement = measurementOf(lines)
			meta.DataMetadata.PointCount = len(lines)
			meta.DataMetadata.UncompressedBytes = len(raw)
			path, derr := writeDLQJSON(r.cfg.DLQDir, meta, f.Payload)
			if derr != nil {
				r.logger.Error("dlq write failed; returning nack", zap.Uint64("seq", f.Seq), zap.Error(derr))
				return protocol.AckFail // DLQ 落盘失败：不能欺骗 ACK，退回重试
			}
			r.metrics.IncDLQ()
			r.logger.Error("poison packet isolated to DLQ",
				zap.Uint64("seq", f.Seq), zap.String("path", path),
				zap.Int("http_status", httpStatus), zap.Error(err))
			// 视为已处理：推进 last_seq，回 0xff 解卡主通道
			r.markDone(f.Seq)
			return protocol.AckSuccess
		}
		r.logger.Error("influx write failed (transient)", zap.Uint64("seq", f.Seq), zap.Int("lines", nLines), zap.Error(err))
		return protocol.AckFail // 可重试：不更新 last_seq，不确认；Sender 重发
	}
	r.metrics.IncWriteOk()
	// 写库成功后无额外去重登记：last_seq 连续推进 + 幂等写入已覆盖重复帧；
	// 并发同 seq 在途重复（滑窗重发窗口内）双写幂等无害。

	// V1.3 中继：写库成功的同时，原始 Line Protocol 写入转发 WAL（
	// 由中继 Sender 发往下一跳；转发失败由 WAL 缓冲重试，不丢数据）。
	// 注意：毒丸（写库失败进 DLQ）不转发，避免下游同样落 DLQ。
	if r.cfg.RelayWAL != nil {
		if _, err := r.cfg.RelayWAL.Append(f.Type, raw); err != nil {
			// C2：append 失败仅记日志会永久丢失该帧（上游已 ACK，重传被去重）。
			// 修复：把 raw lines 落中继专用 DLQ（RelayDLQDir），告警 + 计数。
			r.logger.Error("relay wal append failed, saving to relay dlq", zap.Uint64("seq", f.Seq), zap.Error(err))
			if r.cfg.RelayDLQDir != "" {
				meta := DLQMeta{SeqNum: f.Seq, RetryAttempts: 1}
				meta.ErrorContext.Category = "RELAY_FORWARD_FAILURE"
				meta.ErrorContext.ErrorMessage = err.Error()
				meta.DataMetadata.SourceZone = "Zone_II"
				lines := splitLines(raw)
				meta.DataMetadata.Measurement = measurementOf(lines)
				meta.DataMetadata.PointCount = len(lines)
				meta.DataMetadata.UncompressedBytes = len(raw)
				if _, derr := writeDLQJSON(r.cfg.RelayDLQDir, meta, f.Payload); derr != nil {
					r.logger.Error("relay dlq write failed", zap.Uint64("seq", f.Seq), zap.Error(derr))
				}
			}
			r.metrics.IncRelayDLQ()
		}
	}

	// 写库成功后才推进 last_seq（顺序铁律）
	r.markDone(f.Seq)
	// A5：记录最后落库点时间戳（e2e 延迟指标）
	if r.cfg.LastWriteTs != nil {
		if ts := lastPointTimestamp(raw); ts > 0 {
			r.cfg.LastWriteTs(ts)
		}
	}
	r.logger.Info("frame written", zap.Uint64("seq", f.Seq), zap.Int("lines", nLines))
	return protocol.AckSuccess
}

// markDone 帧处理完成（写库成功或毒丸隔离）：推进 last_seq 并节流持久化。
// seqTracker 只推进连续前缀：跳过的帧等重传补齐——防止帧 k+1 先完成把
// last_seq 推过仍在途的帧 k，重传 k 被 "seq<=last_seq" 吞掉导致丢数据。
// 超大跳跃（>seqJumpLimit，双方持久化重置）直接越过（该区间帧不会再出现）。
func (r *Receiver) markDone(seq uint64) {
	if !r.seqOrd.done(seq) {
		return
	}
	r.advanceSeq(r.seqOrd.load())
}

// addInflight / removeInflight 维护在途帧集合（N6 缺口闭合双保险）。
func (r *Receiver) addInflight(seq uint64) {
	r.inflightMu.Lock()
	r.inflightSeq[seq] = struct{}{}
	r.inflightMu.Unlock()
}

func (r *Receiver) removeInflight(seq uint64) {
	r.inflightMu.Lock()
	delete(r.inflightSeq, seq)
	r.inflightMu.Unlock()
}

// tryCloseGap 尝试闭合永久缺口（N6）：
// 前提是调用方已确认本帧为新连接首帧（frameIdx==0，首帧= sender WAL 头）。
// 若 seq > last+1，则 [last+1, seq-1] 在 sender 侧已 Commit（否则 WAL 头不会
// 推进到 seq），"0xff=已落库" 保证它们已在 receiver 旧进程完成（不可能在途），
// 可安全闭合。双保险：缺口区间必须全部不在途（防多 sender 误配等病态场景）。
func (r *Receiver) tryCloseGap(seq uint64) bool {
	last := uint64(r.lastSeq.Load())
	if seq <= last+1 {
		return false
	}
	// 双保险：缺口区间全部不在途才闭合
	r.inflightMu.Lock()
	defer r.inflightMu.Unlock()
	for s := last + 1; s < seq; s++ {
		if _, infl := r.inflightSeq[s]; infl {
			return false
		}
	}
	r.seqOrd.closeGap(seq - 1)
	r.advanceSeq(r.seqOrd.load())
	return true
}

// advanceSeq 推进 last_seq（只增不减 CAS）+ 节流持久化。
// 持久化节流为每秒最多一次：崩溃窗口内丢失的推进由 Sender 重发 + Influx
// 幂等覆盖兜底（At-Least-Once 不受影响）。
func (r *Receiver) advanceSeq(seq uint64) {
	for {
		cur := r.lastSeq.Load()
		if int64(seq) <= cur || r.lastSeq.CompareAndSwap(cur, int64(seq)) {
			break
		}
	}
	if r.cfg.LastSeqFile == "" {
		return
	}
	r.persistMu.Lock()
	defer r.persistMu.Unlock()
	now := time.Now()
	if now.Before(r.persistAt) {
		return
	}
	r.persistAt = now.Add(time.Second)
	if err := saveLastSeq(r.cfg.LastSeqFile, int64(seq)); err != nil {
		// 持久化失败不阻断：重启后重复帧由 Influx 幂等覆盖 / DLQ 幂等去重
		r.logger.Warn("persist last_seq failed", zap.Uint64("seq", seq), zap.Error(err))
	}
}

// LastSeq 返回已确认最大 seq（测试用）。
func (r *Receiver) LastSeq() int64 { return r.lastSeq.Load() }

// splitLines 按行拆分 Line Protocol（payload 末尾可能带 \n）。
// 仅用于 DLQ 元信息提取等低频路径；写库热路径走 WriteRaw 不再拆行。
func splitLines(raw []byte) []string {
	var lines []string
	for _, l := range strings.Split(string(raw), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// lastPointTimestamp 提取 raw 中最后一行的时间戳（ns）。
// 帧内点按时间升序，最后一行的 ts 即该帧最大点时间（零分配解析）。
func lastPointTimestamp(raw []byte) int64 {
	raw = bytes.TrimRight(raw, "\n\r\t ")
	if len(raw) == 0 {
		return 0
	}
	line := raw
	if i := bytes.LastIndexByte(raw, '\n'); i >= 0 {
		line = raw[i+1:]
	}
	j := bytes.LastIndexByte(line, ' ')
	if j < 0 {
		return 0
	}
	var n int64
	for _, c := range line[j+1:] {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// seqTracker 按序 seq 推进器（N2：恒开）。
// 帧可能乱序完成（流水线/多连接）：只推进连续前缀；大跳跃
// （>seqJumpLimit，双方持久化重置、该区间帧永不再来）直接越过。
// 待补帧由重传自然闭合（go-back-N 重发从失败帧起的所有在途帧）。
type seqTracker struct {
	mu      sync.Mutex
	last    uint64
	pending map[uint64]struct{} // 已完成但非连续的帧
}

// seqPendingLimit pending 上限。go-back-N sender 在失败帧上阻塞、无法产生
// 超过窗口大小的后继完成帧，现实中不可达；到达上限说明缺口是永久性的
// （双方持久化同时丢失），此时只清空 pending（都是已完成帧，清空仅降低
// 去重效率，重传会幂等重写，绝不跳越 last_seq 去吞帧）。
const seqPendingLimit = 65536

func newSeqTracker() *seqTracker {
	return &seqTracker{pending: make(map[uint64]struct{})}
}

// init 设置初始连续值（从 last_seq 文件恢复）。
func (t *seqTracker) init(v uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.last = v
	t.pending = make(map[uint64]struct{})
}

// closeGap 直接推进连续值到 upTo（永久缺口闭合，N6）。
// 调用方必须保证 [last+1, upTo] 的帧已完成（发送方权威判定）。
func (t *seqTracker) closeGap(upTo uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if upTo > t.last {
		t.last = upTo
	}
	for s := range t.pending {
		if s <= t.last {
			delete(t.pending, s)
		}
	}
}

// done 记录 seq 完成；返回是否推进了 last_seq。
func (t *seqTracker) done(seq uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if seq <= t.last {
		return false
	}
	if seq > t.last+seqJumpLimit {
		// 大跳跃：合法 WAL 重置，直接越过（中间帧不会再出现）
		t.last = seq
		t.pending = make(map[uint64]struct{})
		return true
	}
	t.pending[seq] = struct{}{}
	advanced := false
	for {
		if _, ok := t.pending[t.last+1]; !ok {
			break
		}
		delete(t.pending, t.last+1)
		t.last++
		advanced = true
	}
	// N4：溢出时只推进连续前缀 + 清空 pending（保留缺口，绝不跳越）。
	// 重传的已清空帧会幂等重写，正确性不受影响。
	if len(t.pending) > seqPendingLimit {
		zap.L().Error("seqTracker pending overflow: gap never closed (both-side persistence lost?). Clearing completed set; correctness kept by idempotent rewrite",
			zap.Uint64("last_seq", t.last), zap.Int("pending", len(t.pending)))
		t.pending = make(map[uint64]struct{})
	}
	return advanced
}

// load 返回当前连续推进值。
func (t *seqTracker) load() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

// saveLastSeq 原子持久化 last_seq（tmp + rename）。
func saveLastSeq(path string, seq int64) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("receiver: mkdir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", seq)), 0o600); err != nil {
		return fmt.Errorf("receiver: write last_seq: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("receiver: rename last_seq: %w", err)
	}
	return nil
}

func loadLastSeq(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("receiver: read last_seq: %w", err)
	}
	var seq int64
	if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &seq); err != nil {
		return 0, fmt.Errorf("receiver: parse last_seq: %w", err)
	}
	return seq, nil
}
