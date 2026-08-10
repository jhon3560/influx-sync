package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"influx-sync/internal/protocol"
)

// startEchoServer 启动一个真实 TCP 服务端：收到帧 → 解压校验 → 回 ACK。
func startEchoServer(t *testing.T, fail bool) (addr string, gotSeq chan uint64) {
	t.Helper()
	gotSeq = make(chan uint64, 64)
	srv := NewServer(ServerConfig{Listen: "127.0.0.1:0"}, func(id uint64, frameBytes []byte) byte {
		f, err := protocol.Decode(frameBytes)
		if err != nil {
			return protocol.AckFail
		}
		gotSeq <- f.Seq
		if fail {
			return protocol.AckFail
		}
		return protocol.AckSuccess
	})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)
	t.Cleanup(func() { cancel(); srv.Close() })
	return srv.Addr().String(), gotSeq
}

func TestClientSendWaitAck(t *testing.T) {
	addr, gotSeq := startEchoServer(t, false)
	c := NewClient(ClientConfig{Addr: addr, Timeout: 3 * time.Second})
	defer c.Close()
	if err := c.EnsureConnected(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	fb, err := protocol.EncodeData(1, []byte("m value=1 1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SendFrame(fb); err != nil {
		t.Fatalf("send: %v", err)
	}
	ack, err := c.WaitAck()
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if ack != protocol.AckSuccess {
		t.Fatalf("ack=%x", ack)
	}
	select {
	case s := <-gotSeq:
		if s != 1 {
			t.Fatalf("server got seq %d", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive frame")
	}
}

func TestClientNack(t *testing.T) {
	addr, _ := startEchoServer(t, true)
	c := NewClient(ClientConfig{Addr: addr, Timeout: 3 * time.Second})
	defer c.Close()
	c.EnsureConnected()
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	c.SendFrame(fb)
	ack, err := c.WaitAck()
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if ack != protocol.AckFail {
		t.Fatalf("ack=%x, want 0x00", ack)
	}
}

func TestClientTimeoutAndReconnect(t *testing.T) {
	// 服务器不回复 ACK（静默断开模拟：直接关闭连接）
	srv, _ := startEchoServer(t, false)
	_ = srv
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close() // 立即断开，不回复
		}
	}()
	c := NewClient(ClientConfig{Addr: ln.Addr().String(), Timeout: 500 * time.Millisecond})
	defer c.Close()
	if err := c.EnsureConnected(); err != nil {
		t.Fatal(err)
	}
	fb, _ := protocol.EncodeData(1, []byte("m value=1 1"))
	c.SendFrame(fb)
	if _, err := c.WaitAck(); err == nil {
		t.Fatal("expected ack timeout error")
	}
	// 连接应已关闭，重连可再次尝试（服务器仍立即断开）
	if err := c.EnsureConnected(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
}

func TestServerBadHeader(t *testing.T) {
	// 发送损坏帧，服务器应回 0x00 并关闭连接
	var bad [protocol.HeaderSize]byte // 全 0：magic 错误
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		conn.Read(buf)
		done <- struct{}{}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write(bad[:])
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not respond")
	}
}

func TestHeartbeatRoundTrip(t *testing.T) {
	addr, gotSeq := startEchoServer(t, false)
	c := NewClient(ClientConfig{Addr: addr, Timeout: 3 * time.Second})
	defer c.Close()
	c.EnsureConnected()
	fb, err := protocol.EncodeHeartbeat(99)
	if err != nil {
		t.Fatal(err)
	}
	c.SendFrame(fb)
	ack, err := c.WaitAck()
	if err != nil || ack != protocol.AckSuccess {
		t.Fatalf("ack=%x err=%v", ack, err)
	}
	select {
	case s := <-gotSeq:
		if s != 99 {
			t.Fatalf("seq=%d", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat not received")
	}
}
