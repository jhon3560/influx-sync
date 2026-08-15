// Package protocol 实现 ISFP（Influx Sync Frame Protocol）帧协议。
//
// Header 固定 20 字节，Big Endian：
//
//	| Magic(2)=0x5057 | Version(1)=0x01 | Type(1) | Seq(8) | Length(4) | CRC32(4) |
//
// CRC32(IEEE) 只计算 Payload；Payload = gzip(Line Protocol)（TypeData）或
// zstd(Line Protocol)（TypeDataZstd，V1.6）——帧类型即压缩算法标识。
package protocol

import "errors"

const (
	Magic       uint16 = 0x5057 // "PW"
	Version     uint8  = 0x01
	HeaderSize  int    = 20
	MaxFrameLen int    = 1 << 20 // 压缩后单帧上限 1MB（含 Header），受隔离装置单包限制
	// MaxDecompressedLen 解压后原始数据上限（防解压炸弹）。
	// 注意：压缩后 ≤1MB 并不保证解压前 ≤1MB（30000点≈2.4MB 原始），
	// 解压限制必须独立于压缩限制，否则帧会被截断产生毒丸（V1.2.1 实测发现）。
	MaxDecompressedLen int = 16 << 20 // 16MB；正常帧（10000点≈800KB）远小于此
)

// 消息类型。
const (
	TypeData      uint8 = 0x01 // 数据帧（gzip 压缩）
	TypeHeartbeat uint8 = 0x02 // 心跳帧
	TypeControl   uint8 = 0x03 // 控制帧
	TypeDataZstd  uint8 = 0x04 // 数据帧（zstd 压缩，V1.6：更快压缩/解压、更低延迟）
	TypeError     uint8 = 0xFF // 错误帧
)

// 单字节业务 ACK（正向隔离装置仅允许 0x00/0xff 返回）。
const (
	AckSuccess byte = 0xff
	AckFail    byte = 0x00
)

// 协议级错误。
var (
	ErrBadMagic   = errors.New("protocol: bad magic")
	ErrBadVersion = errors.New("protocol: unsupported version")
	ErrBadCRC     = errors.New("protocol: crc mismatch")
	ErrTooLarge   = errors.New("protocol: frame too large")
)

// Frame 解码后的协议帧（Payload 为压缩数据：gzip 或 zstd，按 Type 区分）。
type Frame struct {
	Version uint8
	Type    uint8
	Seq     uint64
	CRC     uint32
	Payload []byte
}

// Header 原始 20 字节头部。
type Header struct {
	Magic   uint16
	Version uint8
	Type    uint8
	Seq     uint64
	Length  uint32
	CRC     uint32
}

// IsHeartbeat 判断是否为心跳帧。
func (f *Frame) IsHeartbeat() bool { return f.Type == TypeHeartbeat }

// IsData 判断是否为数据帧（gzip）。
func (f *Frame) IsData() bool { return f.Type == TypeData }

// IsDataZstd 判断是否为 zstd 数据帧（V1.6）。
func (f *Frame) IsDataZstd() bool { return f.Type == TypeDataZstd }
