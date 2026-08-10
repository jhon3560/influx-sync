// Sender 入口：I 区数据轮询 → WAL → 隔离发送。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/config"
	"influx-sync/internal/influx"
	"influx-sync/internal/logger"
	"influx-sync/internal/monitor"
	"influx-sync/internal/sender"
	"influx-sync/internal/transport"
	"influx-sync/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "sender: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/sender.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.LoadSender(cfgPath)
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Log)
	if err != nil {
		return err
	}
	defer log.Sync()
	log.Info("sender starting",
		zap.String("source", cfg.Source.URL),
		zap.String("database", cfg.Source.Database),
		zap.String("tcp", cfg.TCP.Addr),
		zap.String("wal", cfg.WAL.Path),
	)

	// Influx 源
	influxClient, err := influx.NewClient(cfg.Source)
	if err != nil {
		return err
	}

	// WAL
	walInst, err := wal.Open(cfg.WAL.Path, cfg.SegmentSize())
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	defer walInst.Close()

	// 首次启动（无 checkpoint）：游标初始化为 now-watermark-backfill，
	// 只同步最近数据（backfill 可回填更早的历史）；避免从 epoch 逐窗口爬行
	if walInst.Cursor() == 0 {
		init := time.Now().UnixNano() - int64(cfg.WatermarkDuration()+cfg.BackfillDuration())
		if err := walInst.SetCursor(init); err != nil {
			return fmt.Errorf("init cursor: %w", err)
		}
		log.Info("first start: cursor initialized", zap.Int64("cursor", init))
	}
	log.Info("wal restored",
		zap.Int64("cursor", walInst.Cursor()),
		zap.Int("pending", walInst.PendingCount()),
		zap.Uint64("next_seq", walInst.NextSeq()),
	)

	// 监控指标
	metrics := monitor.New()
	metrics.SetCursor(walInst.Cursor())
	metrics.SetWALPending(int64(walInst.PendingCount()))
	metrics.SetWALBytes(walInst.DiskUsage())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 组件
	poller := sender.NewPoller(influxClient, walInst, metrics, log, cfg.PollerConfig())
	client := transport.NewClient(cfg.ClientConfig())
	senderLoop := sender.NewSender(walInst, client, metrics, log, cfg.SenderLoopConfig())

	go poller.Run(ctx)
	go senderLoop.Run(ctx)

	// V1.2 订阅信号触发：配置 signal_listen 时启动信号接收器（仅信号，不解析内容）
	if cfg.Sync.SignalListen != "" {
		sig := sender.NewSignalListener(poller.Notify, log)
		go func() {
			if err := sig.Run(ctx, cfg.Sync.SignalListen); err != nil {
				log.Error("signal listener failed (fallback to polling)", zap.Error(err))
			}
		}()
	}

	// 指标 HTTP 服务
	metricsSrv := metrics.NewHTTPServer(cfg.Monitor.Addr, cfg.Monitor.Auth())
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil {
			log.Warn("metrics server stopped", zap.Error(err))
		}
	}()
	log.Info("sender started", zap.String("metrics", cfg.Monitor.Addr))

	// 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Info("shutting down", zap.String("signal", s.String()))
	cancel()
	metricsSrv.Shutdown(context.Background())
	client.Close()
	log.Info("sender stopped")
	return nil
}
