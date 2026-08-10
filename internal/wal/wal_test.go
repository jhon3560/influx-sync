package wal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"influx-sync/internal/protocol"
)

func newTestWAL(t *testing.T, segSize int64) *WAL {
	t.Helper()
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal"), segSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestAppendPeekCommit(t *testing.T) {
	w := newTestWAL(t, 0)
	// 追加两帧
	seq1, err := w.Append(protocol.TypeData, []byte("m value=1 1"))
	if err != nil {
		t.Fatalf("Append1: %v", err)
	}
	seq2, err := w.Append(protocol.TypeData, []byte("m value=2 2"))
	if err != nil {
		t.Fatalf("Append2: %v", err)
	}
	if seq1 != 1 || seq2 != 2 {
		t.Fatalf("seq: %d %d", seq1, seq2)
	}
	if w.PendingCount() != 2 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	// Peek 顺序返回
	s, fb, err := w.Peek()
	if err != nil || s != 1 {
		t.Fatalf("peek1: seq=%d err=%v", s, err)
	}
	f, err := protocol.Decode(fb)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	raw, _ := f.Decompress()
	if !bytes.Equal(raw, []byte("m value=1 1")) {
		t.Fatalf("payload=%q", raw)
	}
	// commit 1
	if err := w.Commit(1); err != nil {
		t.Fatalf("commit1: %v", err)
	}
	if w.PendingCount() != 1 {
		t.Fatalf("pending after commit=%d", w.PendingCount())
	}
	s, _, err = w.Peek()
	if err != nil || s != 2 {
		t.Fatalf("peek2: seq=%d err=%v", s, err)
	}
	if err := w.Commit(2); err != nil {
		t.Fatalf("commit2: %v", err)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending after commit2=%d", w.PendingCount())
	}
	if _, _, err := w.Peek(); err != ErrEmpty {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}

func TestOutOfOrderCommitRejected(t *testing.T) {
	w := newTestWAL(t, 0)
	w.Append(protocol.TypeData, []byte("a"))
	w.Append(protocol.TypeData, []byte("b"))
	if err := w.Commit(2); err == nil {
		t.Fatal("expected out-of-order error")
	}
}

func TestReopenRecovery(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	seqs := []uint64{}
	for i := 0; i < 5; i++ {
		seq, err := w.Append(protocol.TypeData, []byte(fmt.Sprintf("m value=%d %d", i, i)))
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	w.SetCursor(12345)
	// 确认前两帧，再"崩溃"重开
	w.Commit(seqs[0])
	w.Commit(seqs[1])
	w.Close()

	w2, err := Open(walDir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if w2.Cursor() != 12345 {
		t.Fatalf("cursor=%d", w2.Cursor())
	}
	if w2.PendingCount() != 3 {
		t.Fatalf("pending=%d", w2.PendingCount())
	}
	if w2.NextSeq() != 6 {
		t.Fatalf("next_seq=%d", w2.NextSeq())
	}
	// 剩余帧顺序正确
	want := []uint64{seqs[2], seqs[3], seqs[4]}
	for i, ws := range want {
		s, _, err := w2.Peek()
		if err != nil || s != ws {
			t.Fatalf("peek[%d]: seq=%d err=%v", i, s, err)
		}
		if err := w2.Commit(ws); err != nil {
			t.Fatalf("commit[%d]: %v", i, err)
		}
	}
	if w2.PendingCount() != 0 {
		t.Fatalf("pending=%d", w2.PendingCount())
	}
}

func TestSegmentRolloverAndDelete(t *testing.T) {
	// 段大小 4KB，迫使滚动多个段
	w := newTestWAL(t, 4096)
	payload := bytes.Repeat([]byte("x"), 1024)
	var seqs []uint64
	for i := 0; i < 30; i++ {
		seq, err := w.Append(protocol.TypeData, payload)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	if w.PendingCount() != 30 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	// 全部顺序确认
	for _, s := range seqs {
		if err := w.Commit(s); err != nil {
			t.Fatalf("commit %d: %v", s, err)
		}
	}
	// 段文件应全部删除（.lock 与 checkpoint 保留）
	entries, _ := os.ReadDir(w.dir)
	for _, e := range entries {
		if e.Name() == "checkpoint" || e.Name() == "checkpoint.tmp" || e.Name() == ".lock" {
			continue
		}
		t.Fatalf("unexpected file left: %s", e.Name())
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
}

func TestSegmentRolloverRecovery(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, _ := Open(walDir, 4096)
	payload := bytes.Repeat([]byte("y"), 1024)
	var seqs []uint64
	for i := 0; i < 20; i++ {
		seq, _ := w.Append(protocol.TypeData, payload)
		seqs = append(seqs, seq)
	}
	// 确认前 5 帧（可能跨段）
	for i := 0; i < 5; i++ {
		w.Commit(seqs[i])
	}
	w.Close()

	w2, err := Open(walDir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.PendingCount() != 15 {
		t.Fatalf("pending=%d", w2.PendingCount())
	}
	s, _, err := w2.Peek()
	if err != nil || s != seqs[5] {
		t.Fatalf("peek seq=%d err=%v want=%d", s, err, seqs[5])
	}
	// 全部确认后仍能继续追加
	for i := 5; i < 20; i++ {
		w2.Commit(seqs[i])
	}
	seq, err := w2.Append(protocol.TypeData, payload)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if seq != 21 {
		t.Fatalf("seq=%d", seq)
	}
}

func TestCursorPersistAndRegress(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, _ := Open(walDir, 0)
	w.SetCursor(100)
	w.Close()
	w2, _ := Open(walDir, 0)
	defer w2.Close()
	if w2.Cursor() != 100 {
		t.Fatalf("cursor=%d", w2.Cursor())
	}
	if err := w2.SetCursor(50); err == nil {
		t.Fatal("expected regress error")
	}
}

func TestMoveToDLQ(t *testing.T) {
	w := newTestWAL(t, 0)
	seq, _ := w.Append(protocol.TypeData, []byte("m value=1 1"))
	if err := w.MoveToDLQ(seq, "field type conflict"); err != nil {
		t.Fatalf("MoveToDLQ: %v", err)
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	// dlq 文件存在且内容正确
	dlqDir := filepath.Join(filepath.Dir(w.dir), "dlq")
	framePath := filepath.Join(dlqDir, fmt.Sprintf("seq-%020d.frame", seq))
	if _, err := os.Stat(framePath); err != nil {
		t.Fatalf("dlq frame missing: %v", err)
	}
	metaPath := filepath.Join(dlqDir, fmt.Sprintf("seq-%020d.txt", seq))
	meta, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("dlq meta missing: %v", err)
	}
	if !bytes.Contains(meta, []byte("field type conflict")) {
		t.Fatalf("meta=%q", meta)
	}
}

func TestCheckpointAtomicFormat(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, _ := Open(walDir, 0)
	w.Append(protocol.TypeData, []byte("m value=1 1"))
	w.SetCursor(999)
	w.Close()
	data, err := os.ReadFile(filepath.Join(walDir, "checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatalf("checkpoint not json: %v", err)
	}
	if cp.CursorNs != 999 || cp.NextSeq != 2 {
		t.Fatalf("cp=%+v", cp)
	}
}

func TestConcurrentAppendPeek(t *testing.T) {
	w := newTestWAL(t, 0)
	var wg sync.WaitGroup
	// 并发追加
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if _, err := w.Append(protocol.TypeData, []byte(fmt.Sprintf("m,g=%d value=%d 1", g, i))); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if w.PendingCount() != 100 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	// 顺序确认全部
	seen := map[uint64]bool{}
	for {
		s, _, err := w.Peek()
		if err == ErrEmpty {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if seen[s] {
			t.Fatalf("duplicate seq %d", s)
		}
		seen[s] = true
		w.Commit(s)
	}
	if len(seen) != 100 {
		t.Fatalf("acked=%d", len(seen))
	}
}

func TestConcurrentCommit(t *testing.T) {
	// 验证并发乱序提交不会破坏 WAL 结构；顺序提交最终全部成功
	w := newTestWAL(t, 0)
	var seqs []uint64
	for i := 0; i < 10; i++ {
		s, _ := w.Append(protocol.TypeData, []byte("m value=1 1"))
		seqs = append(seqs, s)
	}
	var wg sync.WaitGroup
	for _, s := range seqs {
		wg.Add(1)
		go func(s uint64) {
			defer wg.Done()
			w.Commit(s) // 乱序提交：绝大多数应被拒绝，但不允许破坏状态
		}(s)
	}
	wg.Wait()
	// 顺序提交剩余帧，最终必须全部确认
	for _, s := range seqs {
		if err := w.Commit(s); err != nil && !strings.Contains(err.Error(), "out-of-order") {
			t.Fatalf("commit %d: %v", s, err)
		}
	}
	if w.PendingCount() != 0 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
}

func TestOpenLockExclusive(t *testing.T) {
	dir := t.TempDir()
	w1, err := Open(filepath.Join(dir, "wal"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Close()
	// 第二个实例打开同一 WAL 应被拒绝
	if _, err := Open(filepath.Join(dir, "wal"), 0); err == nil {
		t.Fatal("second open must fail (lock held)")
	}
	// 关闭后可以再打开
	w1.Close()
	w2, err := Open(filepath.Join(dir, "wal"), 0)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	w2.Close()
}
