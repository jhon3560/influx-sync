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
	DedupCap    int         // LRU 容量
	DLQDir      string      // 毒丸死信目录（空=禁用 DLQ）
	RelayWAL    *wal.WAL    // 中继转发 WAL（V1.3；空=不启用中继）
	RelayDLQDir string      // 中继转发失败转存目录（C2：空=退化为仅告警）
	OrderedSeq  bool        // 并发写库模式（A2 流水线）下 last_seq 按序推进（防跳序吞重试）
	LastWriteTs func(int64) // 可选：落库点时间戳回调（A5 e2e 延迟指标），nil=不回调
}

// seqJumpLimit 允许的最大 seq 跳跃（防外部/异常帧污染 last_seq）。
const seqJumpLimit uint64 = 100000

// Receiver 帧处理器：Decode → 去重 → 写 Influx → ACK。
// 依据协议：写库成功后才回 0xff，保证“ACK = 已落库”。
type Receiver struct {
	client  *influx.Client
	dedup   *LRU
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     Config
	lastSeq atomic.Int64 // 已成功处理的最大 seq（内存）
	seqOrd  *seqTracker  // OrderedSeq 模式下的按序 seq 推进器（nil=直接 max 推进）

	persistMu sync.Mutex
	persistAt time.Time // last_seq 持久化节流（每秒最多一次）
}

// New 创建 Receiver。
func New(client *influx.Client, metrics *monitor.Metrics, logger *zap.Logger, cfg Config) (*Receiver, error) {
	r := &Receiver{client: client, dedup: NewLRU(cfg.DedupCap), metrics: metrics, logger: logger, cfg: cfg}
	if cfg.OrderedSeq {
		r.seqOrd = newSeqTracker()
	}
	if cfg.LastSeqFile != "" {
		seq, err := loadLastSeq(cfg.LastSeqFile)
		if err != nil {
			return nil, err
		}
		r.lastSeq.Store(seq)
		if r.seqOrd != nil {
			r.seqOrd.init(uint64(seq))
		}
		logger.Info("receiver restored last_seq", zap.Int64("seq", seq))
	}
	return r, nil
}

// HandleFrame 处理一帧，返回 ACK 字节。线程安全（多连接/并发流水线可调用）。
func (r *Receiver) HandleFrame(connID uint64, frameBytes []byte) byte {
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

	// 重复检测：已成功处理的旧 seq 直接确认
	if f.Seq <= uint64(r.lastSeq.Load()) {
		r.metrics.IncDup()
		r.logger.Debug("duplicate seq (<=last_seq)", zap.Uint64("seq", f.Seq))
		return protocol.AckSuccess
	}

	// seq 跳跃告警但不拒绝：Influx 幂等覆盖保证最终一致，
	// 拒绝会导致 Sender 停等重发同一帧、链路永久卡死（如 last_seq 文件被
	// 清理后重启，Sender WAL 仍有高位 seq 积压的场景）。
	if f.Seq > uint64(r.lastSeq.Load())+1 {
		if f.Seq > uint64(r.lastSeq.Load())+seqJumpLimit {
			r.logger.Error("seq jump too large, frame accepted anyway (idempotent overwrite)",
				zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()))
		} else {
			r.logger.Warn("seq jump", zap.Uint64("seq", f.Seq), zap.Int64("last_seq", r.lastSeq.Load()))
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
	// 去重登记必须在写库成功之后（修复：此前写前登记导致瞬时失败→重发同 seq
	// 被 LRU 吞掉直接回 0xff 的数据永久丢失缺陷）。
	r.dedup.CheckAndAdd(f.Seq)

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
// OrderedSeq 模式（并发流水线）下按序推进：只推进连续前缀，跳过的帧等
// 重传补齐——防止帧 k+1 先完成把 last_seq 推过仍在途的帧 k，重传 k 被
// "seq<=last_seq" 吞掉导致丢数据。
func (r *Receiver) markDone(seq uint64) {
	if r.seqOrd != nil {
		if adv := r.seqOrd.done(seq); !adv {
			return
		}
		seq = r.seqOrd.load()
	}
	r.advanceSeq(seq)
}

// advanceSeq 推进 last_seq（内存 + 节流持久化）。
// 持久化节流为每秒最多一次：崩溃窗口内丢失的推进由 Sender 重发 + Influx
// 幂等覆盖兜底（At-Least-Once 不受影响）。
func (r *Receiver) advanceSeq(seq uint64) {
	r.lastSeq.Store(int64(seq))
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

// seqTracker 并发写库模式（A2 流水线）下的按序 seq 推进器。
// 帧可能乱序完成：只推进连续前缀；大跳跃（>seqJumpLimit，WAL 重置）直接越过。
// 待补帧由重传自然闭合（go-back-N 重发从失败帧起的所有在途帧）。
type seqTracker struct {
	mu      sync.Mutex
	last    uint64
	pending map[uint64]struct{}
}

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
	// 防无界：pending 超过上限视为永久缺口（双方持久化均丢失的病理场景）
	if len(t.pending) > 65536 {
		for s := range t.pending {
			if s > t.last {
				t.last = s
			}
		}
		t.pending = make(map[uint64]struct{})
		advanced = true
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
