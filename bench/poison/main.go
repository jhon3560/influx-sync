// poison: 毒丸注入工具——向 Receiver 发送一帧 CRC 合法但 Line Protocol 非法的数据帧，
// 用于验证死信隔离（DLQ）链路：Receiver 应落盘 DLQ 并回 0xff，主通道不被卡死。
// 用法: poison -addr 127.0.0.1:7777 [-bad "invalid line protocol !!!"]
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"influx-sync/internal/protocol"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "receiver tcp addr")
	bad := flag.String("bad", "this is not valid line protocol ###", "invalid line protocol payload")
	seq := flag.Uint64("seq", 999999, "frame seq (需大于 receiver last_seq)")
	flag.Parse()

	frameBytes, err := protocol.EncodeData(*seq, []byte(*bad))
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		os.Exit(1)
	}
	conn, err := net.DialTimeout("tcp", *addr, 3*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(frameBytes); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
	var ack [1]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil {
		fmt.Fprintln(os.Stderr, "read ack:", err)
		os.Exit(1)
	}
	if ack[0] == protocol.AckSuccess {
		fmt.Println("ACK=0xff (deceptive ack: poison isolated, channel continues)")
	} else {
		fmt.Println("ACK=0x00 (retry)")
	}
}
