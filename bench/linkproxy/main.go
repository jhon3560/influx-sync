// linkproxy: TCP 字节计数代理。
// 用法: linkproxy -listen :28081 -target 127.0.0.1:28101 -metrics :28091
//
//	正向流量（listen→target）计入 tx_bytes_total，反向（target→listen）计入 rx_bytes_total。
//	每连接一个 goroutine 转发，计数器线程安全；/metrics 输出文本格式供采样。
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"
)

var (
	txBytes  atomic.Uint64
	rxBytes  atomic.Uint64
	conns    atomic.Int64
	peerName string
)

func pipe(dst, src net.Conn, count *atomic.Uint64) {
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
			count.Add(uint64(n))
		}
		if err != nil {
			return
		}
	}
}

func handle(c net.Conn, target string) {
	defer c.Close()
	conns.Add(1)
	defer conns.Add(-1)
	up, err := net.Dial("tcp", target)
	if err != nil {
		log.Printf("dial target %s: %v", target, err)
		return
	}
	defer up.Close()
	go pipe(c, up, &rxBytes) // 反向
	pipe(up, c, &txBytes)    // 正向
}

func main() {
	listen := flag.String("listen", ":28081", "监听地址")
	target := flag.String("target", "127.0.0.1:28101", "转发目标")
	metricsAddr := flag.String("metrics", ":28091", "指标监听地址")
	flag.Parse()
	peerName = *target

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("linkproxy: %s -> %s (metrics %s)", *listen, *target, *metricsAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "linkproxy_tx_bytes_total %d\nlinkproxy_rx_bytes_total %d\nlinkproxy_conns %d\n",
			txBytes.Load(), rxBytes.Load(), conns.Load())
	})
	go func() { log.Fatal(http.ListenAndServe(*metricsAddr, mux)) }()

	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handle(c, *target)
	}
}
