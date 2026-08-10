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
	IdleSleep         time.Duration // 空闲轮询间隔，默认 200ms
}

// Sender 停等发送器：WAL 取帧 → TCP 发送 → 等 ACK → 提交/重试/DLQ。
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
	return &Sender{wal: w, client: client, metrics: metrics, logger: logger, cfg: cfg}
}

// Run 阻塞运行，直到 ctx 取消。
func (s *Sender) Run(ctx context.Context) {
	retryCount := 0
	lastHeartbeat := time.Now()
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
				time.Sleep(s.cfg.IdleSleep)
				continue
			}
			s.logger.Error("wal peek failed", zap.Error(err))
			time.Sleep(s.cfg.IdleSleep)
			continue
		}
		s.metrics.SetWALPending(int64(s.wal.PendingCount()))

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

// sendHeartbeat 发送心跳帧（失败仅告警，不重试）。
func (s *Sender) sendHeartbeat() {
	fb, err := protocol.EncodeHeartbeat(s.wal.NextSeq())
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
