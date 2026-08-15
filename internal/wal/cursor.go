package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 回填策略哨兵值（记录于 checkpoint.BackfillNs）。
const (
	BackfillNoneNs int64 = 0  // 仅实时（无回填边界）
	BackfillAllNs  int64 = -1 // 全量（边界=触发时探测的库内最早数据）
)

// checkpoint 持久化状态，保存在 wal 目录下的 checkpoint 文件。
// 采用 tmp + fsync + rename 原子替换，避免掉电损坏。
type checkpoint struct {
	CursorNs   int64  `json:"cursor_ns"`   // 逻辑游标：已进入 WAL 的最大数据时间（ns）
	NextSeq    uint64 `json:"next_seq"`    // 下一个帧序号
	SegStart   int    `json:"seg_start"`   // 最老未删除段的序号
	AckedBytes int64  `json:"acked_bytes"` // SegStart 段内已确认字节偏移
	BackfillNs int64  `json:"backfill_ns"` // V1.7 回填策略：0=仅实时/-1=全量/>0=有界时长(ns)
}

func checkpointPath(dir string) string { return filepath.Join(dir, "checkpoint") }

// loadCheckpoint 读取 checkpoint；文件不存在时返回零值。
// legacy 表示文件存在但不含 backfill_ns 字段（V1.6 及更早版本写入）——
// 供 ApplyBackfillPolicy 区分"存量部署升级"（只记录配置、不回拨）。
func loadCheckpoint(dir string) (cp checkpoint, legacy bool, err error) {
	data, err := os.ReadFile(checkpointPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return cp, false, nil
		}
		return cp, false, fmt.Errorf("wal: read checkpoint: %w", err)
	}
	legacy = !strings.Contains(string(data), "backfill_ns")
	if err := json.Unmarshal(data, &cp); err != nil {
		return cp, false, fmt.Errorf("wal: parse checkpoint: %w", err)
	}
	return cp, legacy, nil
}

// saveCheckpoint 原子持久化：写临时文件 → fsync → rename → fsync 目录。
func saveCheckpoint(dir string, cp checkpoint) error {
	tmp := checkpointPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("wal: create checkpoint tmp: %w", err)
	}
	data, err := json.Marshal(cp)
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: marshal checkpoint: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("wal: write checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("wal: fsync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("wal: close checkpoint: %w", err)
	}
	if err := os.Rename(tmp, checkpointPath(dir)); err != nil {
		return fmt.Errorf("wal: rename checkpoint: %w", err)
	}
	return syncDir(dir)
}

// syncDir fsync 目录，保证 rename 持久化。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
