// Receiver 入口：III 区接收帧 → 校验/去重 → 批量写目标 Influx → ACK。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/config"
	"influx-sync/internal/influx"
	"influx-sync/internal/logger"
	"influx-sync/internal/monitor"
	"influx-sync/internal/receiver"
	"influx-sync/internal/sender"
	"influx-sync/internal/transport"
	"influx-sync/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "receiver: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/receiver.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.LoadReceiver(cfgPath)
	if err != nil {
		return err
	}
	log, err := logger.New(cfg.Log)
	if err != nil {
		return err
	}
	defer log.Sync()
	log.Info("receiver starting",
		zap.String("target", cfg.Target.URL),
		zap.String("database", cfg.Target.Database),
		zap.String("listen", cfg.TCP.Listen),
	)

	// 目标 Influx
	influxClient, err := influx.NewClient(cfg.Target)
	if err != nil {
		return err
	}

	// 监控指标（中继 Sender 与主链路共用）
	metrics := monitor.New()

	// V1.3 中继：配置 relay.addr 时，打开转发 WAL 并组装中继 Sender
	var relaySenderLoop *sender.Sender
	if cfg.Relay.Addr != "" {
		relayWAL, err := wal.Open(cfg.Relay.WALDir, 64<<20)
		if err != nil {
			return fmt.Errorf("relay wal open %s: %w", cfg.Relay.WALDir, err)
		}
		cfg.RelayWAL = relayWAL
		// C2：转发失败转存目录（默认 <wal_dir>/../relay_dlq）
		if cfg.Relay.DLQDir == "" {
			cfg.Relay.DLQDir = filepath.Join(filepath.Dir(cfg.Relay.WALDir), "relay_dlq")
		}
		// C5：relay.timeout 配置项真正生效（原先硬编码 10s）
		relayTimeout := cfg.RelayTimeout()
		relayClient := transport.NewClient(transport.ClientConfig{
			Addr:        cfg.Relay.Addr,
			Timeout:     relayTimeout,
			DialTimeout: relayTimeout,
		})
		relaySenderLoop = sender.NewSender(relayWAL, relayClient, metrics, log, sender.SenderConfig{})
		log.Info("relay enabled", zap.String("addr", cfg.Relay.Addr),
			zap.String("wal", cfg.Relay.WALDir),
			zap.String("dlq", cfg.Relay.DLQDir),
			zap.Duration("timeout", relayTimeout))
	}

	// 帧处理器（校验/去重/写库/ACK）；A5：落库点时间戳 → e2e 延迟指标
	rcfg := cfg.ReceiverConfig()
	rcfg.LastWriteTs = metrics.SetLastWriteTs
	handler, err := receiver.New(influxClient, metrics, log, rcfg)
	if err != nil {
		return err
	}

	// TCP 服务器
	srv := transport.NewServer(cfg.ServerConfig(), func(connID uint64, frameBytes []byte) byte {
		return handler.HandleFrame(connID, frameBytes)
	})
	if err := srv.Listen(); err != nil {
		return fmt.Errorf("listen %s: %w", cfg.TCP.Listen, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Serve(ctx)

	// V1.3 中继 Sender 循环（转发 WAL → 下一跳）
	if relaySenderLoop != nil {
		go relaySenderLoop.Run(ctx)
	}

	// 指标 HTTP 服务
	metricsSrv := metrics.NewHTTPServer(cfg.Monitor.Addr, cfg.Monitor.Auth())
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil {
			log.Warn("metrics server stopped", zap.Error(err))
		}
	}()
	log.Info("receiver started", zap.String("listen", cfg.TCP.Listen), zap.String("metrics", cfg.Monitor.Addr))

	// 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Info("shutting down", zap.String("signal", s.String()))
	cancel()
	srv.Close()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	metricsSrv.Shutdown(shutCtx)
	log.Info("receiver stopped")
	return nil
}
