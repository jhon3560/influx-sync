// Receiver 入口：III 区接收帧 → 校验/去重 → 批量写目标 Influx → ACK。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"influx-sync/internal/config"
	"influx-sync/internal/influx"
	"influx-sync/internal/logger"
	"influx-sync/internal/monitor"
	"influx-sync/internal/receiver"
	"influx-sync/internal/transport"
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

	// 帧处理器（校验/去重/写库/ACK）
	metrics := monitor.New()
	handler, err := receiver.New(influxClient, metrics, log, cfg.ReceiverConfig())
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
	metricsSrv.Shutdown(context.Background())
	log.Info("receiver stopped")
	return nil
}
