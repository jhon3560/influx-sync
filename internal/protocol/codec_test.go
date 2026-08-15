package protocol

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	lp := []byte("power_measure,plant=A001,point=P001 value=220.5 1720000000000000000\n")
	seq := uint64(10001)
	frameBytes, err := EncodeData(seq, lp)
	if err != nil {
		t.Fatalf("EncodeData: %v", err)
	}
	if len(frameBytes) <= HeaderSize {
		t.Fatalf("unexpected frame len %d", len(frameBytes))
	}
	f, err := Decode(frameBytes)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if f.Seq != seq || f.Type != TypeData || f.Version != Version {
		t.Fatalf("bad frame fields: %+v", f)
	}
	raw, err := f.Decompress()
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(raw, lp) {
		t.Fatalf("payload mismatch: got %q want %q", raw, lp)
	}
}

func TestDecodeBadCRC(t *testing.T) {
	lp := []byte("m value=1 1")
	frameBytes, _ := EncodeData(7, lp)
	frameBytes[len(frameBytes)-1] ^= 0xff // 破坏 payload 末字节
	_, err := Decode(frameBytes)
	if err == nil || !strings.Contains(err.Error(), "crc") {
		t.Fatalf("expected crc error, got %v", err)
	}
}

func TestDecodeBadMagic(t *testing.T) {
	lp := []byte("m value=1 1")
	frameBytes, _ := EncodeData(7, lp)
	frameBytes[0] = 0x00 // 破坏 magic
	_, err := Decode(frameBytes)
	if err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("expected magic error, got %v", err)
	}
}

func TestDecodeLengthMismatch(t *testing.T) {
	lp := []byte("m value=1 1")
	frameBytes, _ := EncodeData(7, lp)
	// 截短 payload
	_, err := Decode(frameBytes[:len(frameBytes)-2])
	if err == nil {
		t.Fatal("expected length mismatch error")
	}
}

func TestDecodeTooLarge(t *testing.T) {
	// 手工构造声称超大 Length 的 header
	buf := make([]byte, HeaderSize)
	putHeader(buf, Header{Magic: Magic, Version: Version, Type: TypeData, Seq: 1, Length: uint32(MaxFrameLen)})
	if _, err := ParseHeader(buf); err == nil {
		t.Fatal("expected too large error")
	}
}

func TestParseHeaderFields(t *testing.T) {
	frameBytes, _ := EncodeData(12345678901234, []byte("x"))
	h, err := ParseHeader(frameBytes[:HeaderSize])
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.Magic != Magic || h.Version != Version || h.Type != TypeData || h.Seq != 12345678901234 {
		t.Fatalf("bad header: %+v", h)
	}
	// 校验 CRC 与直接计算一致
	if h.CRC != crc32.ChecksumIEEE(frameBytes[HeaderSize:]) {
		t.Fatal("crc mismatch")
	}
}

func TestEncodeHeartbeat(t *testing.T) {
	fb, err := EncodeHeartbeat(42)
	if err != nil {
		t.Fatalf("EncodeHeartbeat: %v", err)
	}
	h, err := ParseHeader(fb[:HeaderSize])
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.Type != TypeHeartbeat || h.Length != 0 || h.Seq != 42 {
		t.Fatalf("bad heartbeat: %+v", h)
	}
	f, err := Decode(fb)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !f.IsHeartbeat() {
		t.Fatal("expected heartbeat")
	}
}

func TestEncodeTooLarge(t *testing.T) {
	// 随机数据不可压缩，压缩后仍超限
	big := make([]byte, MaxFrameLen*2)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeData(1, big); err == nil {
		t.Fatal("expected too large error")
	}
}

func TestDecompressTooLargeRejected(t *testing.T) {
	// 解压后超过 MaxDecompressedLen 的帧：必须报错而非静默截断
	//（截断会破坏最后一行的完整性，产生数据损坏）
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	big := bytes.Repeat([]byte("a"), MaxDecompressedLen+1024)
	if _, err := zw.Write(big); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	fb := encodeRaw(TypeData, 1, buf.Bytes(), crc32.ChecksumIEEE(buf.Bytes()))
	f, err := Decode(fb)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, err := f.Decompress(); err == nil {
		t.Fatal("expected decompress too-large error, got nil (silent truncation)")
	}
}

func TestEncodeDecodeZstd(t *testing.T) {
	payload := []byte("telemetry,plant=A001,point=P0001 value=1.5 1720000000000000000\n")
	for i := 0; i < 100; i++ {
		payload = append(payload, []byte("telemetry,plant=A001,point=P0002 value=2 1720000000000000001\n")...)
	}
	fb, err := EncodeDataZstd(7, payload)
	if err != nil {
		t.Fatal(err)
	}
	f, err := Decode(fb)
	if err != nil {
		t.Fatal(err)
	}
	if !f.IsDataZstd() || f.Seq != 7 {
		t.Fatalf("type=%x seq=%d", f.Type, f.Seq)
	}
	out, err := f.Decompress()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(payload) {
		t.Fatal("zstd roundtrip payload mismatch")
	}
	// 与 gzip 帧同 payload 解压结果一致
	fbg, _ := EncodeData(7, payload)
	fg, _ := Decode(fbg)
	outg, err := fg.Decompress()
	if err != nil || string(outg) != string(payload) {
		t.Fatal("gzip roundtrip mismatch")
	}
	// zstd 压缩率不劣于 gzip（同数据对比；若劣化说明选型有误）
	if len(fb) > len(fbg) {
		t.Fatalf("zstd frame %d > gzip frame %d", len(fb), len(fbg))
	}
}

func TestEncodeUnsupportedType(t *testing.T) {
	if _, err := Encode(TypeControl, 1, []byte("x")); err == nil {
		t.Fatal("unsupported type must fail")
	}
	if _, err := Encode(TypeError, 1, []byte("x")); err == nil {
		t.Fatal("unsupported type must fail")
	}
}

func TestDecompressZstdBombGuard(t *testing.T) {
	// 构造小压缩帧解压超过 16MB 上限的 zstd 流（全零块）
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	big := make([]byte, MaxDecompressedLen+1024)
	compressed := enc.EncodeAll(big, nil)
	fb := encodeRaw(TypeDataZstd, 1, compressed, crc32.ChecksumIEEE(compressed))
	f, err := Decode(fb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Decompress(); err == nil {
		t.Fatal("zstd bomb over limit must fail")
	}
}
