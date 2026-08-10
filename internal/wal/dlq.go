package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// writeDLQ 将毒丸帧转存死信目录：
//
//	dlq/seq-%020d.frame   完整帧字节
//	dlq/seq-%020d.txt     reason（含时间戳）
func writeDLQ(dir string, seq uint64, frameBytes []byte, reason string) error {
	framePath := filepath.Join(dir, fmt.Sprintf("seq-%020d.frame", seq))
	if err := os.WriteFile(framePath, frameBytes, 0o600); err != nil {
		return fmt.Errorf("wal: write dlq frame: %w", err)
	}
	meta := fmt.Sprintf("seq=%d\ntime=%s\nreason=%s\n", seq, time.Now().Format(time.RFC3339), reason)
	metaPath := filepath.Join(dir, fmt.Sprintf("seq-%020d.txt", seq))
	if err := os.WriteFile(metaPath, []byte(meta), 0o600); err != nil {
		return fmt.Errorf("wal: write dlq reason: %w", err)
	}
	return nil
}

// dlqSize 统计死信目录占用字节数。
func dlqSize(dir string) int64 {
	var n int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err == nil {
			n += info.Size()
		}
	}
	return n
}
