// Package transport 实现 Sender 侧 TCP 客户端与 Receiver 侧 TCP 服务器。
package transport

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ClientConfig TCP 客户端配置。
type ClientConfig struct {
	Addr        string        // 对端地址 host:port
	Timeout     time.Duration // 读写超时
	DialTimeout time.Duration // 连接超时
}

// Client Sender 侧 TCP 客户端（停等协议：SendFrame 后必须 WaitAck）。
type Client struct {
	cfg  ClientConfig
	mu   sync.Mutex
	conn net.Conn
}

// NewClient 创建客户端。默认超时 10s。
func NewClient(cfg ClientConfig) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = cfg.Timeout
	}
	return &Client{cfg: cfg}
}

// EnsureConnected 保证连接存在；断开时自动重连。
func (c *Client) EnsureConnected() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", c.cfg.Addr, c.cfg.DialTimeout)
	if err != nil {
		return fmt.Errorf("tcp: dial %s: %w", c.cfg.Addr, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(15 * time.Second)
	}
	c.conn = conn
	return nil
}

// SendFrame 发送完整帧字节。
func (c *Client) SendFrame(frameBytes []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("tcp: not connected")
	}
	c.conn.SetWriteDeadline(time.Now().Add(c.cfg.Timeout))
	// 循环写全量：net.Conn.Write 可能部分写（大帧 1MB 时可能发生）
	for written := 0; written < len(frameBytes); {
		n, err := c.conn.Write(frameBytes[written:])
		if err != nil {
			c.closeLocked()
			return fmt.Errorf("tcp: write: %w", err)
		}
		written += n
	}
	return nil
}

// WaitAck 读取 1 字节 ACK（0xff 成功 / 0x00 失败）。读超时视为连接故障并关闭。
func (c *Client) WaitAck() (byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return 0, fmt.Errorf("tcp: not connected")
	}
	c.conn.SetReadDeadline(time.Now().Add(c.cfg.Timeout))
	var buf [1]byte
	n, err := io.ReadFull(c.conn, buf[:])
	if err != nil {
		c.closeLocked()
		return 0, fmt.Errorf("tcp: read ack: %w", err)
	}
	if n != 1 {
		c.closeLocked()
		return 0, fmt.Errorf("tcp: empty ack")
	}
	return buf[0], nil
}

// Close 关闭连接。
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

func (c *Client) closeLocked() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
