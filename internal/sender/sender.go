package sender

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/monitor"
	"influx-sync/internal/protocol"
	"influx-sync/internal/transport"
	"influx-sync/internal/wal"
)

// SenderConfig 发送主循环配置。
type SenderConfig struct {
	MaxRetry          int           // 连续失败上限，超过转 DLQ，默认 10
	BackoffBase       time.Duration // 退避基数，默认 1s
	BackoffMax        time.Duration // 退避上限，默认 60s
	HeartbeatInterval time.Duration // 心跳间隔，默认 30s
	IdleSleep         time.Duration // 空闲轮询间隔（WAL 通知失效时的兜底），默认 200ms
	Pipeline          int           // 滑窗大小（A1 实验项），默认 1=停等。>1 时同连接多帧在途
}

// Sender 停等发送器：WAL 取帧 → TCP 发送 → 等 ACK → 提交/重试/DLQ。
// Pipeline>1 时进入滑窗模式（go-back-N）：吞吐 = W×batch/RTT，
// 需与隔离装置确认允许同连接多请求在途后再开启。
type Sender struct {
	wal     *wal.WAL
	client  *transport.Client
	metrics *monitor.Metrics
	logger  *zap.Logger
	cfg     SenderConfig
}

// NewSender 创建发送器。
func NewSender(w *wal.WAL, client *transport.Client, metrics *monitor.Metrics, logger *zap.Logger, cfg SenderConfig) *Sender {
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 10
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 60 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.IdleSleep <= 0 {
		cfg.IdleSleep = 200 * time.Millisecond
	}
	if cfg.Pipeline <= 0 {
		cfg.Pipeline = 1
	}
	return &Sender{wal: w, client: client, metrics: metrics, logger: logger, cfg: cfg}
}

// Run 阻塞运行，直到 ctx 取消。
func (s *Sender) Run(ctx context.Context) {
	retryCount := 0
	lastHeartbeat := time.Now()
	// 最后 Peek 帧缓存：重试/重发时避免每次重读盘 + 分配（小项加固）
	var cachedSeq uint64
	var cachedBytes []byte
	haveCache := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		seq, frameBytes, err := s.wal.Peek()
		if err != nil {
			if errors.Is(err, wal.ErrEmpty) {
				// 空闲：按需心跳，维持隔离通道
				s.metrics.SetWALPending(0)
				if time.Since(lastHeartbeat) >= s.cfg.HeartbeatInterval {
					s.sendHeartbeat()
					lastHeartbeat = time.Now()
				}
				// A3：WAL append 通知优先唤醒（新数据零延迟）；IdleSleep 仅为兜底
				select {
				case <-ctx.Done():
					return
				case <-s.wal.NotifyCh():
				case <-time.After(s.cfg.IdleSleep):
				}
				continue
			}
			s.logger.Error("wal peek failed", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.cfg.IdleSleep):
			}
			continue
		}
		s.metrics.SetWALPending(int64(s.wal.PendingCount()))

		// 重试同帧时复用缓存（Peek 每次持锁读盘，且 1MB 分配）
		if haveCache && cachedSeq == seq {
			frameBytes = cachedBytes
		} else {
			cachedSeq, cachedBytes = seq, frameBytes
			haveCache = true
		}

		backoff := s.backoff(retryCount)
		if retryCount > 0 {
			s.logger.Info("retry frame", zap.Uint64("seq", seq), zap.Int("retry", retryCount), zap.Duration("backoff", backoff))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		if err := s.client.EnsureConnected(); err != nil {
			s.logger.Warn("connect failed", zap.Error(err))
			s.metrics.IncRetry()
			retryCount++
			continue
		}

		if s.cfg.Pipeline > 1 {
			retryCount = s.runPipeline(ctx, seq, frameBytes, retryCount)
			continue
		}

		s.metrics.IncSend()
		if err := s.client.SendFrame(frameBytes); err != nil {
			s.logger.Warn("send failed", zap.Uint64("seq", seq), zap.Error(err))
			s.metrics.IncRetry()
			retryCount++
			continue
		}
		ack, err := s.client.WaitAck()
		if err != nil {
			s.logger.Warn("ack wait failed", zap.Uint64("seq", seq), zap.Error(err))
			s.metrics.IncRetry()
			retryCount++
			continue
		}

		switch ack {
		case protocol.AckSuccess:
			if err := s.wal.Commit(seq); err != nil {
				s.logger.Error("wal commit failed", zap.Uint64("seq", seq), zap.Error(err))
				continue
			}
			s.metrics.IncAckOk()
			s.logger.Info("ack success", zap.Uint64("seq", seq))
			retryCount = 0
		case protocol.AckFail:
			s.metrics.IncAckFail()
			retryCount++
			if retryCount >= s.cfg.MaxRetry {
				// 重试超限：只告警，绝不丢弃。0x00 均为可重试错误（毒丸已由
				// Receiver 侧 DLQ 隔离并回 0xff），丢数据违反 At-Least-Once 红线。
				s.logger.Error("frame keep retrying (nack threshold reached, NOT dropped)",
					zap.Uint64("seq", seq), zap.Int("retries", retryCount))
				retryCount = s.cfg.MaxRetry // 退避保持封顶
			}
		default:
			s.logger.Warn("invalid ack byte", zap.Uint64("seq", seq), zap.Uint8("ack", ack))
			retryCount++
		}
	}
}

// runPipeline 滑窗发送一轮（A1 实验项，Pipeline>1）：连续发 W 帧后按序读回
// W 个 ACK；第 k 个 0x00 触发 go-back-N（从 k 起重发）。返回更新后的 retryCount。
// 协议兼容：每个帧仍恰好对应一个响应字节，只是同连接多帧在途。
//
// N1 修复：0x00/非法 ACK/发送失败一律视为**连接级失败**——立即关闭连接重连后
// 从 nackAt 起重发整个尾窗。关闭连接排干了第 1 轮 i+1..W-1 帧的陈旧 ACK 字节，
// 避免重发后 ACK 流错位导致"提交从未写库的帧"（At-Least-Once 红线）。
// receiver 侧幂等写入 + 按序 lastSeq 去重保证重发不产生重复计数。
func (s *Sender) runPipeline(ctx context.Context, seq uint64, frameBytes []byte, retryCount int) int {
	w := s.cfg.Pipeline
	// 一次 PeekBatch 取满窗口（避免首个 Peek 结果被丢弃 + 重复读盘 W×1MB）
	frames, err := s.wal.PeekBatch(w)
	if err != nil {
		frames = []wal.FrameData{{Seq: seq, Bytes: frameBytes}}
	}
	for {
		// 发送全部在途帧；失败即中断（连接已坏，后续发送必败）
		sendNack := -1
		for i, f := range frames {
			select {
			case <-ctx.Done():
				return retryCount
			default:
			}
			if i > 0 {
				backoff := s.backoff(retryCount)
				if retryCount > 0 {
					s.logger.Info("pipeline retry", zap.Uint64("seq", f.Seq), zap.Int("retry", retryCount), zap.Duration("backoff", backoff))
				}
				select {
				case <-ctx.Done():
					return retryCount
				case <-time.After(backoff):
				}
			}
			s.metrics.IncSend()
			if err := s.client.SendFrame(f.Bytes); err != nil {
				s.logger.Warn("pipeline send failed", zap.Uint64("seq", f.Seq), zap.Error(err))
				s.metrics.IncRetry()
				retryCount++
				if retryCount > s.cfg.MaxRetry {
					retryCount = s.cfg.MaxRetry
				}
				sendNack = i
				break // 连接已断：0..i-1 的 ACK 随连接丢失，从 i 起重发
			}
		}
		if sendNack >= 0 {
			if err := s.client.EnsureConnected(); err != nil {
				s.logger.Warn("pipeline reconnect failed", zap.Error(err))
				s.metrics.IncRetry()
				retryCount++
				if retryCount > s.cfg.MaxRetry {
					retryCount = s.cfg.MaxRetry
				}
			}
			frames = frames[sendNack:]
			continue
		}
		// 按序读回 ACK：0x00 处从该帧起重发（go-back-N）
		nackAt := -1
		for i := range frames {
			ack, err := s.client.WaitAck()
			if err != nil {
				s.logger.Warn("pipeline ack wait failed", zap.Uint64("seq", frames[i].Seq), zap.Error(err))
				s.metrics.IncRetry()
				retryCount++
				if retryCount > s.cfg.MaxRetry {
					retryCount = s.cfg.MaxRetry
				}
				nackAt = i
				break // WaitAck 失败已自动关闭连接：陈旧 ACK 流随连接消亡
			}
			switch ack {
			case protocol.AckSuccess:
				if err := s.wal.Commit(frames[i].Seq); err != nil {
					s.logger.Error("wal commit failed", zap.Uint64("seq", frames[i].Seq), zap.Error(err))
					continue
				}
				s.metrics.IncAckOk()
				s.logger.Info("pipeline ack success", zap.Uint64("seq", frames[i].Seq))
				retryCount = 0
			case protocol.AckFail:
				s.metrics.IncAckFail()
				retryCount++
				if retryCount >= s.cfg.MaxRetry {
					s.logger.Error("frame keep retrying (nack threshold reached, NOT dropped)",
						zap.Uint64("seq", frames[i].Seq), zap.Int("retries", retryCount))
					retryCount = s.cfg.MaxRetry
				}
				nackAt = i
				// N1：0x00 = 连接级失败。关连接重连再重发——重连天然清空
				// 线上残留的 i+1..W-1 陈旧 ACK，杜绝 ACK 错位提交
				s.client.Close()
			default:
				s.logger.Warn("invalid ack byte", zap.Uint64("seq", frames[i].Seq), zap.Uint8("ack", ack))
				retryCount++
				nackAt = i
				s.client.Close()
			}
			if nackAt >= 0 {
				break
			}
		}
		if nackAt < 0 {
			return retryCount // 全窗确认完成
		}
		// go-back-N：重连后从 nackAt 起重发尾窗（窗口缩窄不 refill，
		// 避免每轮重读盘；下一轮外层循环 PeekBatch 补满）
		if err := s.client.EnsureConnected(); err != nil {
			s.logger.Warn("pipeline reconnect failed", zap.Error(err))
			s.metrics.IncRetry()
			retryCount++
			if retryCount > s.cfg.MaxRetry {
				retryCount = s.cfg.MaxRetry
			}
		}
		frames = frames[nackAt:]
	}
}

// sendHeartbeat 发送心跳帧（失败仅告警，不重试）。
// 心跳 seq 固定为 0（不消耗数据帧 seq 空间，避免语义混淆）。
func (s *Sender) sendHeartbeat() {
	fb, err := protocol.EncodeHeartbeat(0)
	if err != nil {
		return
	}
	if err := s.client.EnsureConnected(); err != nil {
		return
	}
	if err := s.client.SendFrame(fb); err != nil {
		s.logger.Debug("heartbeat send failed", zap.Error(err))
		return
	}
	if _, err := s.client.WaitAck(); err != nil {
		s.logger.Debug("heartbeat ack failed", zap.Error(err))
		return
	}
	s.metrics.IncHeartbeat()
	s.logger.Debug("heartbeat ok")
}

// backoff 指数退避：1s,2s,4s...60s 封顶。
func (s *Sender) backoff(n int) time.Duration {
	if n <= 0 {
		return 0
	}
	d := s.cfg.BackoffBase
	for i := 1; i < n; i++ {
		d *= 2
		if d >= s.cfg.BackoffMax {
			return s.cfg.BackoffMax
		}
	}
	return d
}
