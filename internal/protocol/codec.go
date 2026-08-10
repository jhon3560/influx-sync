package protocol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// EncodeData 将 Line Protocol 文本压缩编码为完整帧字节（Header+Payload）。
// 返回的字节可直接写入 TCP 或 WAL。
func EncodeData(seq uint64, lineProtocol []byte) ([]byte, error) {
	return Encode(TypeData, seq, lineProtocol)
}

// EncodeHeartbeat 编码心跳帧（空 Payload）。
func EncodeHeartbeat(seq uint64) ([]byte, error) {
	return Encode(TypeHeartbeat, seq, nil)
}

// Encode 按类型编码帧：gzip(BestSpeed) → CRC → Header。
// 心跳帧不压缩：Payload 为空，Length=0。
func Encode(typ uint8, seq uint64, payload []byte) ([]byte, error) {
	if typ == TypeHeartbeat {
		if len(payload) != 0 {
			return nil, fmt.Errorf("protocol: heartbeat must have empty payload")
		}
		return encodeRaw(typ, seq, nil, 0), nil
	}
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
	compressed := buf.Bytes()
	if HeaderSize+len(compressed) > MaxFrameLen {
		return nil, fmt.Errorf("protocol: frame too large: %d bytes", HeaderSize+len(compressed))
	}
	return encodeRaw(typ, seq, compressed, crc32.ChecksumIEEE(compressed)), nil
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

// Decompress 解压 Payload 为原始 Line Protocol 文本。
func (f *Frame) Decompress() ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(f.Payload))
	if err != nil {
		return nil, fmt.Errorf("protocol: gzip open: %w", err)
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, int64(MaxFrameLen)))
	if err != nil {
		return nil, fmt.Errorf("protocol: gzip read: %w", err)
	}
	return out, nil
}
