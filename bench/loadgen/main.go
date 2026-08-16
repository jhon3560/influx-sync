// loadgen: 高吞吐压测数据生成器。
// 每秒生成 rate 个点（plant × point 网格），按 batch 批量写入源 InfluxDB。
// 用法: loadgen -rate 50000 -duration 120 -url http://127.0.0.1:18086
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"influx-sync/internal/influx"
)

func main() {
	rate := flag.Int("rate", 50000, "points per second")
	duration := flag.Int("duration", 60, "duration in seconds")
	url := flag.String("url", "http://127.0.0.1:18086", "source influx url")
	db := flag.String("db", "power", "database")
	batch := flag.Int("batch", 5000, "points per write batch")
	workers := flag.Int("workers", 8, "concurrent write workers")
	flag.Parse()

	if *rate <= 0 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "rate and duration must be positive")
		os.Exit(1)
	}

	// 网格设计：plants 个厂 × pointsPerPlant 个点 ≈ rate
	plants := *rate / 500
	if plants < 1 {
		plants = 1
	}
	pointsPerPlant := (*rate + plants - 1) / plants
	actualRate := plants * pointsPerPlant
	fmt.Printf("loadgen: %d plants × %d points = %d pts/s (target %d), %d workers, batch=%d\n",
		plants, pointsPerPlant, actualRate, *rate, *workers, *batch)

	client, err := influx.NewClient(influx.Config{URL: *url, Database: *db, Timeout: "30s"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}

	ch := make(chan []string, (*workers)*2)
	done := make(chan struct{}) // 任一 worker 失败退出后关闭，防主循环永久阻塞
	var doneOnce sync.Once
	var wg sync.WaitGroup
	var writeErr atomic.Value
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for lines := range ch {
				if err := client.WriteLines(ctx, lines); err != nil {
					writeErr.Store(err)
					doneOnce.Do(func() { close(done) })
					return
				}
			}
		}()
	}

	// 预生成 tag 模板（避免每批重复拼接）
	tagTpl := make([]string, 0, plants*pointsPerPlant)
	for p := 1; p <= plants; p++ {
		for pt := 1; pt <= pointsPerPlant; pt++ {
			tagTpl = append(tagTpl, fmt.Sprintf("telemetry,plant=A%03d,point=P%04d", p, pt))
		}
	}

	var total atomic.Int64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	deadline := time.Now().Add(time.Duration(*duration) * time.Second)
	lastTotal := int64(0)

send:
	for now := range ticker.C {
		// 每秒生成一批：时间戳在秒内均匀铺开（每点独立纳秒，模拟真实采集）
		secStart := now.Truncate(time.Second).UnixNano()
		sec := secStart / 1e9
		step := int64(1e9) / int64(len(tagTpl)) // 点间时间步长
		if step < 1 {
			step = 1
		}
		for i := 0; i < len(tagTpl); i += *batch {
			end := i + *batch
			if end > len(tagTpl) {
				end = len(tagTpl)
			}
			lines := make([]string, 0, end-i)
			for j, tpl := range tagTpl[i:end] {
				val := float64(sec%100000) + 1000 // 可预测值
				ts := secStart + int64(i+j)*step
				lines = append(lines, fmt.Sprintf("%s value=%f,quality=1i %d", tpl, val, ts))
			}
			total.Add(int64(len(lines)))
			// worker 失败退出时停止发送（否则无消费者，阻塞永久挂起）
			select {
			case ch <- lines:
			case <-done:
				break send
			}
		}
		// 每秒统计
		cur := total.Load()
		fmt.Printf("[%s] %.0f pts/s (cumulative %d)\n", now.Format("15:04:05"),
			float64(cur-lastTotal), cur)
		lastTotal = cur
		if now.After(deadline) {
			break
		}
	}
	close(ch)
	wg.Wait()
	if err, ok := writeErr.Load().(error); ok {
		fmt.Fprintln(os.Stderr, "write error:", err)
		os.Exit(1)
	}
	fmt.Printf("done: %d points in %ds (%.0f pts/s)\n", total.Load(), *duration, float64(total.Load())/float64(*duration))
}
