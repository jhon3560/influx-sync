package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"influx-sync/internal/protocol"
)

// ServerConfig TCP 服务器配置。
type ServerConfig struct {
	Listen      string        // 监听地址 host:port
	ReadTimeout time.Duration // 单帧读取超时（含心跳间隔余量）
}

// FrameHandler 处理一帧完整数据，返回 ACK 字节（protocol.AckSuccess/AckFail）。
type FrameHandler func(connID uint64, frameBytes []byte) byte

// Server Receiver 侧 TCP 服务器：逐连接逐帧读取 → handler → 回 ACK。
type Server struct {
	cfg     ServerConfig
	handler FrameHandler
	ln      net.Listener
	connSeq atomic.Uint64
	conns   sync.Map // connID -> net.Conn
}

// NewServer 创建服务器。
func NewServer(cfg ServerConfig, handler FrameHandler) *Server {
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	return &Server{cfg: cfg, handler: handler}
}

// Listen 开始监听。
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("tcp: listen %s: %w", s.cfg.Listen, err)
	}
	s.ln = ln
	return nil
}

// Addr 返回实际监听地址。
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve 阻塞接受连接，直到 ctx 取消。
func (s *Server) Serve(ctx context.Context) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			slog.Warn("tcp accept error", "err", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		id := s.connSeq.Add(1)
		s.conns.Store(id, conn)
		go s.handleConn(ctx, id, conn)
	}
}

// Close 关闭监听与所有连接。
func (s *Server) Close() {
	if s.ln != nil {
		s.ln.Close()
	}
	s.conns.Range(func(_, v interface{}) bool {
		v.(net.Conn).Close()
		return true
	})
}

func (s *Server) handleConn(ctx context.Context, id uint64, conn net.Conn) {
	defer func() {
		conn.Close()
		s.conns.Delete(id)
	}()
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
	}
	var headBuf [protocol.HeaderSize]byte
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		if _, err := io.ReadFull(conn, headBuf[:]); err != nil {
			return // 连接关闭/超时
		}
		head, err := protocol.ParseHeader(headBuf[:])
		if err != nil {
			slog.Warn("bad frame header", "conn", id, "err", err)
			s.writeAck(conn, protocol.AckFail)
			return // 头部损坏：无法继续同步（关闭连接，让 sender 重连重发）
		}
		payload := make([]byte, head.Length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		frameBytes := make([]byte, protocol.HeaderSize+len(payload))
		copy(frameBytes, headBuf[:])
		copy(frameBytes[protocol.HeaderSize:], payload)
		ack := s.handler(id, frameBytes)
		if !s.writeAck(conn, ack) {
			return
		}
	}
}

func (s *Server) writeAck(conn net.Conn, ack byte) bool {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := conn.Write([]byte{ack})
	return err == nil
}

// readFrameLen 读取帧记录长度头（供测试与扩展使用）。
func readFrameLen(r io.Reader) (int, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint32(buf[:])), nil
}
