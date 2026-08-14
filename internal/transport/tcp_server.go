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
	MaxInflight int           // 每连接在途帧上限（A2 流水线），<=0 时默认 8
	MaxConns    int           // 最大并发连接数，<=0 不限制
}

// FrameHandler 处理一帧完整数据，返回 ACK 字节（protocol.AckSuccess/AckFail）。
// 可并发调用（A2：并发写库）；ACK 仍由 Server 按帧到达顺序写回。
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
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 8
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
		if s.cfg.MaxConns > 0 {
			cur := 0
			s.conns.Range(func(_, _ interface{}) bool {
				cur++
				return cur < s.cfg.MaxConns+1 // 提前终止：数到上限即可
			})
			if cur >= s.cfg.MaxConns {
				slog.Warn("tcp conn limit reached, closing new connection", "limit", s.cfg.MaxConns)
				conn.Close()
				continue
			}
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

// connPipeline 单连接 ACK 按序写回器（A2）：
// handler 可并发执行（写库 RTT 不计入链路 RTT），但 ACK 必须按帧到达顺序
// 写回——帧 k 的 ACK 仅在 0..k-1 的 ACK 全部写回后输出，保持
// "0xff = 已落库"且响应字节数与顺序与停等模式完全一致（wire 兼容）。
type connPipeline struct {
	mu      sync.Mutex
	cond    *sync.Cond
	next    int64          // 下一个待写回的帧序号（连接内递增，与协议 seq 无关）
	results map[int64]byte // 已完成 handler 的结果
	closed  bool           // 连接已关闭：终止等待
}

func newConnPipeline() *connPipeline {
	p := &connPipeline{results: make(map[int64]byte)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *connPipeline) setResult(idx int64, ack byte) {
	p.mu.Lock()
	p.results[idx] = ack
	p.cond.Broadcast()
	p.mu.Unlock()
}

// nextResult 等待并取回序号 idx 的结果；closed 时返回 ok=false。
func (p *connPipeline) nextResult(idx int64) (byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if ack, ok := p.results[idx]; ok {
			delete(p.results, idx)
			return ack, true
		}
		if p.closed {
			return 0, false
		}
		p.cond.Wait()
	}
}

func (p *connPipeline) close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

// advance 推进已写回 ACK 的位置（解锁在途窗口护栏）。
func (p *connPipeline) advance() {
	p.mu.Lock()
	p.next++
	p.cond.Broadcast()
	p.mu.Unlock()
}

// awaitInflight 等待在途窗口腾出位置；连接关闭返回 false。
func (p *connPipeline) awaitInflight(idx int64, max int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for idx-p.next >= int64(max) {
		if p.closed {
			return false
		}
		p.cond.Wait()
	}
	return true
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
	pipe := newConnPipeline()
	defer pipe.close()

	// ACK 写回协程：按序输出（帧 k 写库完成且 k-1 已回才回它的 ACK）
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		var idx int64
		for {
			ack, ok := pipe.nextResult(idx)
			if !ok {
				return
			}
			if !s.writeAck(conn, ack) {
				pipe.close()
				return
			}
			pipe.advance() // 推进在途窗口（护栏依赖此计数）
			idx++
		}
	}()

	var idx int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		var headBuf [protocol.HeaderSize]byte
		if _, err := io.ReadFull(conn, headBuf[:]); err != nil {
			return // 连接关闭/超时
		}
		head, err := protocol.ParseHeader(headBuf[:])
		if err != nil {
			slog.Warn("bad frame header", "conn", id, "err", err)
			s.writeAck(conn, protocol.AckFail)
			return // 头部损坏：无法继续同步（关闭连接，让 sender 重连重发）
		}
		// 单次分配：Header+Payload 一次 make（原实现 payload+frameBytes 两次）
		frameBytes := make([]byte, protocol.HeaderSize+int(head.Length))
		copy(frameBytes, headBuf[:])
		if _, err := io.ReadFull(conn, frameBytes[protocol.HeaderSize:]); err != nil {
			return
		}
		// 在途窗口护栏（停等 sender 下恒为 1 帧在途，行为不变）
		if !pipe.awaitInflight(idx, s.cfg.MaxInflight) {
			return
		}
		go func(i int64, fb []byte) {
			pipe.setResult(i, s.handler(id, fb))
		}(idx, frameBytes)
		idx++
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
