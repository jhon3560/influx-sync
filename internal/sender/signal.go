package sender

import (
	"context"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// SignalListener 订阅信号接收器（V1.2）。
//
// 原理：源 InfluxDB 的 SUBSCRIPTION 在每次写入成功后，会把数据副本异步推送到
// 本端点。本组件**不解析、不使用推送内容**——只把它当作"源库有新数据"的信号，
// 立即通知 Poller 查询（数据本身仍由 Poller 从源库按游标/窗口查询，完整性
// 100% 依赖 WAL+游标，与订阅通道是否可靠无关）。
//
// 负载特性：
//   - 推送频率 = 写入请求数（batch 数），与点数无关；
//   - 收到请求即回 204（不读 body），处理成本微秒级；
//   - 信号经 coalescing（Poller 忙时丢弃）合并，高频推送不会导致查询风暴。
type SignalListener struct {
	notify func() // 非阻塞发信号（Poller 忙时丢弃=合并，无害）
	logger *zap.Logger
}

// NewSignalListener 创建信号接收器。notify 为 Poller 的信号回调。
func NewSignalListener(notify func(), logger *zap.Logger) *SignalListener {
	return &SignalListener{notify: notify, logger: logger}
}

// ServeHTTP 实现 http.Handler：确认"有数据写入"后立即应答，不解析 body。
func (l *SignalListener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if l.notify != nil {
		l.notify()
	}
	// 排空 body（限制大小）：否则源侧 keep-alive 失效、连接 churn
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
	w.WriteHeader(http.StatusNoContent)
}

// Run 启动 HTTP 服务并阻塞，直到 ctx 取消。
func (l *SignalListener) Run(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           l,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	l.logger.Info("signal listener started", zap.String("addr", addr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
