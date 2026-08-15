package protocol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// EncodeData 将 Line Protocol 文本以 gzip 压缩编码为完整帧字节（Header+Payload）。
// 保留兼容入口；新代码请直接使用 Encode(TypeDataZstd, ...)（V1.6 默认 zstd）。
func EncodeData(seq uint64, lineProtocol []byte) ([]byte, error) {
	return Encode(TypeData, seq, lineProtocol)
}

// EncodeDataZstd 以 zstd 压缩编码数据帧（V1.6：更快压缩/解压，降 CPU 降延迟）。
func EncodeDataZstd(seq uint64, lineProtocol []byte) ([]byte, error) {
	return Encode(TypeDataZstd, seq, lineProtocol)
}

// EncodeHeartbeat 编码心跳帧（空 Payload）。
func EncodeHeartbeat(seq uint64) ([]byte, error) {
	return Encode(TypeHeartbeat, seq, nil)
}

// 共享 zstd 编码器：EncodeAll 可并发调用（klauspost 文档保证），避免每帧
// 重建 encoder（zstd encoder 初始化开销远大于 gzip writer）。
var zstdEnc = func() *zstd.Encoder {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		panic(err)
	}
	return e
}()

// zstd 解码器池：Decoder 的 DecodeAll 非并发安全，按帧串行使用后归还。
var zstdDecPool = sync.Pool{New: func() interface{} { d, _ := zstd.NewReader(nil); return d }}

// Encode 按类型编码帧：TypeData→gzip(BestSpeed)、TypeDataZstd→zstd(SpeedFastest)
// → CRC → Header。心跳帧不压缩：Payload 为空，Length=0。
// 帧类型即压缩算法标识（V1.6）：接收端按类型解压，中继按类型透传，零协商成本。
func Encode(typ uint8, seq uint64, payload []byte) ([]byte, error) {
	if typ == TypeHeartbeat {
		if len(payload) != 0 {
			return nil, fmt.Errorf("protocol: heartbeat must have empty payload")
		}
		return encodeRaw(typ, seq, nil, 0), nil
	}
	// 压缩前先校验原始大小（V1.2.2）：防止 payload 超过解压上限被截断
	if len(payload) > MaxDecompressedLen {
		return nil, fmt.Errorf("protocol: payload too large: %d bytes (max %d), reduce batch_points", len(payload), MaxDecompressedLen)
	}
	var compressed []byte
	var err error
	switch typ {
	case TypeData:
		compressed, err = gzipCompress(payload)
	case TypeDataZstd:
		compressed = zstdEnc.EncodeAll(payload, make([]byte, 0, len(payload)/4+64))
	default:
		return nil, fmt.Errorf("protocol: unsupported frame type 0x%02x", typ)
	}
	if err != nil {
		return nil, err
	}
	if HeaderSize+len(compressed) > MaxFrameLen {
		return nil, fmt.Errorf("protocol: frame too large: %d bytes", HeaderSize+len(compressed))
	}
	return encodeRaw(typ, seq, compressed, crc32.ChecksumIEEE(compressed)), nil
}

func gzipCompress(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("protocol: gzip init: %w", err)
	}
	if _, err := zw.Write(payload); err != nil {
		return nil, fmt.Errorf("protocol: gzip write: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("protocol: gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

func encodeRaw(typ uint8, seq uint64, payload []byte, crc uint32) []byte {
	head := make([]byte, HeaderSize)
	putHeader(head, Header{
		Magic:   Magic,
		Version: Version,
		Type:    typ,
		Seq:     seq,
		Length:  uint32(len(payload)),
		CRC:     crc,
	})
	out := make([]byte, HeaderSize+len(payload))
	copy(out, head)
	copy(out[HeaderSize:], payload)
	return out
}

// putHeader 将 Header 按 Big Endian 写入 20 字节缓冲。
func putHeader(dst []byte, h Header) {
	binary.BigEndian.PutUint16(dst[0:2], h.Magic)
	dst[2] = h.Version
	dst[3] = h.Type
	binary.BigEndian.PutUint64(dst[4:12], h.Seq)
	binary.BigEndian.PutUint32(dst[12:16], h.Length)
	binary.BigEndian.PutUint32(dst[16:20], h.CRC)
}

// ParseHeader 解析 20 字节头部并做基础校验（Magic/Version/Length 上限）。
func ParseHeader(buf []byte) (Header, error) {
	var h Header
	if len(buf) < HeaderSize {
		return h, fmt.Errorf("protocol: header too short: %d", len(buf))
	}
	h.Magic = binary.BigEndian.Uint16(buf[0:2])
	h.Version = buf[2]
	h.Type = buf[3]
	h.Seq = binary.BigEndian.Uint64(buf[4:12])
	h.Length = binary.BigEndian.Uint32(buf[12:16])
	h.CRC = binary.BigEndian.Uint32(buf[16:20])

	if h.Magic != Magic {
		return h, fmt.Errorf("%w: got 0x%04x", ErrBadMagic, h.Magic)
	}
	if h.Version != Version {
		return h, fmt.Errorf("%w: got %d", ErrBadVersion, h.Version)
	}
	if HeaderSize+int(h.Length) > MaxFrameLen {
		return h, fmt.Errorf("%w: length=%d", ErrTooLarge, h.Length)
	}
	return h, nil
}

// Decode 由完整帧字节（Header+Payload）解码并校验 CRC。
func Decode(frameBytes []byte) (Frame, error) {
	if len(frameBytes) < HeaderSize {
		return Frame{}, fmt.Errorf("protocol: frame too short: %d", len(frameBytes))
	}
	h, err := ParseHeader(frameBytes[:HeaderSize])
	if err != nil {
		return Frame{}, err
	}
	if len(frameBytes) != HeaderSize+int(h.Length) {
		return Frame{}, fmt.Errorf("protocol: length mismatch: header=%d got=%d", h.Length, len(frameBytes)-HeaderSize)
	}
	payload := frameBytes[HeaderSize:]
	if crc32.ChecksumIEEE(payload) != h.CRC {
		return Frame{}, fmt.Errorf("%w: seq=%d", ErrBadCRC, h.Seq)
	}
	return Frame{
		Version: h.Version,
		Type:    h.Type,
		Seq:     h.Seq,
		CRC:     h.CRC,
		Payload: payload,
	}, nil
}

// Decompress 按帧类型解压 Payload 为原始 Line Protocol 文本
// （TypeData→gzip、TypeDataZstd→zstd）。
// 解压上限为 MaxDecompressedLen（16MB），独立于压缩帧上限（1MB）——
// 防止压缩后 ≤1MB 但原始数据 >1MB 的帧被截断（V1.2.2 修复）。
// 超限时返回错误而非静默截断（截断会破坏最后一行的完整性）。
func (f *Frame) Decompress() ([]byte, error) {
	var out []byte
	var err error
	switch f.Type {
	case TypeData:
		out, err = gunzip(f.Payload)
	case TypeDataZstd:
		d := zstdDecPool.Get().(*zstd.Decoder)
		defer zstdDecPool.Put(d)
		if err := d.Reset(bytes.NewReader(f.Payload)); err != nil {
			return nil, fmt.Errorf("protocol: zstd reset: %w", err)
		}
		// 与 gzip 路径同款防超限：LimitedReader 多读 1 字节检测炸弹/截断
		lr := &io.LimitedReader{R: d, N: int64(MaxDecompressedLen) + 1}
		out, err = io.ReadAll(lr)
		if err != nil {
			err = fmt.Errorf("protocol: zstd decode: %w", err)
		}
	default:
		return nil, fmt.Errorf("protocol: unsupported frame type 0x%02x", f.Type)
	}
	if err != nil {
		return nil, err
	}
	if len(out) > MaxDecompressedLen {
		return nil, fmt.Errorf("protocol: decompressed payload exceeds %d bytes", MaxDecompressedLen)
	}
	return out, nil
}

// gunzip 解压并检测超限（多读 1 字节：读满上限后仍有数据 = 超限）。
func gunzip(payload []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("protocol: gzip open: %w", err)
	}
	defer zr.Close()
	lr := &io.LimitedReader{R: zr, N: int64(MaxDecompressedLen) + 1}
	out, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("protocol: gzip read: %w", err)
	}
	return out, nil
}
