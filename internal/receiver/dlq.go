package receiver

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"influx-sync/internal/influx"
)

// DLQMeta 死信报文 JSON 规范（依据《死信隔离与反压机制逻辑》）。
// 文件：<dlq_dir>/dlq_{seq}_{timestamp}.json
type DLQMeta struct {
	DLQID         string `json:"dlq_id"`
	CreatedAt     string `json:"created_at"`
	SeqNum        uint64 `json:"seq_num"`
	RetryAttempts int    `json:"retry_attempts"`
	ErrorContext  struct {
		Category     string `json:"category"`    // PERMANENT_ERROR / TRANSIENT_ERROR
		HTTPStatus   int    `json:"http_status"` // 0 表示非 HTTP 错误
		ErrorMessage string `json:"error_message"`
	} `json:"error_context"`
	DataMetadata struct {
		SourceZone        string `json:"source_zone"`
		Measurement       string `json:"measurement"`
		PointCount        int    `json:"point_count"`
		UncompressedBytes int    `json:"uncompressed_bytes"`
	} `json:"data_metadata"`
	// FrameType 原始帧类型（protocol.TypeData=0x01 gzip / TypeDataZstd=0x04 zstd）：
	// payload_gzip_base64 存的是**该类型压缩后的**原始 payload，重放时必须按此
	// 类型解压（V1.7.1：修复 zstd 帧存入 gzip 名字段导致的重放歧义）。
	FrameType         uint8  `json:"frame_type,omitempty"`
	PayloadGzipBase64 string `json:"payload_gzip_base64"`
}

// writeDLQJSON 将毒丸帧序列化为 JSON 落盘（Payload 以 gzip Base64 保存）。
// 返回文件路径。
func writeDLQJSON(dir string, meta DLQMeta, gzipPayload []byte) (string, error) {
	meta.PayloadGzipBase64 = base64.StdEncoding.EncodeToString(gzipPayload)
	if meta.DLQID == "" {
		meta.DLQID = fmt.Sprintf("DLQ_%s_SEQ%d", time.Now().Format("20060102"), meta.SeqNum)
	}
	if meta.CreatedAt == "" {
		meta.CreatedAt = time.Now().Format(time.RFC3339)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("dlq: mkdir %s: %w", dir, err)
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", fmt.Errorf("dlq: marshal: %w", err)
	}
	// 名字带纳秒后缀：同 seq 一秒内重试两次不再互相覆盖
	name := fmt.Sprintf("dlq_%d_%s_%d.json", meta.SeqNum, time.Now().Format("20060102T150405"), time.Now().UnixNano())
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", fmt.Errorf("dlq: write %s: %w", path, err)
	}
	return path, nil
}

// classifyWriteError 错误分类器：区分可重试（Transient）与永久（Permanent）错误。
// 永久错误（HTTP 400/413 等客户端错误、解析类错误）→ 毒丸，直接进 DLQ。
// 其余（500/503/超时/网络）→ 可重试，回 0x00。
// 优先使用 influx 包的 typed error（*WriteHTTPError，C3：不依赖错误文案）；
// 字符串解析仅作为非本客户端错误的兼容路径。
func classifyWriteError(err error) (permanent bool, httpStatus int, category string) {
	var he *influx.WriteHTTPError
	if errors.As(err, &he) {
		switch {
		case he.StatusCode >= 400 && he.StatusCode < 500:
			return true, he.StatusCode, "PERMANENT_ERROR"
		case he.StatusCode >= 500:
			return false, he.StatusCode, "TRANSIENT_ERROR"
		default:
			return false, he.StatusCode, "TRANSIENT_ERROR"
		}
	}
	msg := err.Error()
	// 兼容旧错误文案格式: "influx: write http 400: ..."
	if idx := strings.Index(msg, "write http "); idx >= 0 {
		rest := msg[idx+len("write http "):]
		var code int
		if _, err := fmt.Sscanf(rest, "%d", &code); err == nil {
			switch {
			case code >= 400 && code < 500:
				return true, code, "PERMANENT_ERROR"
			case code >= 500:
				return false, code, "TRANSIENT_ERROR"
			}
		}
	}
	// 非 HTTP 错误（超时/连接失败）→ 可重试
	return false, 0, "TRANSIENT_ERROR"
}

// measurementOf 从首行 Line Protocol 提取 measurement（逗号/空格前的部分）。
func measurementOf(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	line := strings.TrimSpace(lines[0])
	if i := strings.IndexAny(line, ", "); i > 0 {
		return line[:i]
	}
	return line
}
