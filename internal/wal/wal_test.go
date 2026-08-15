package wal

import (
	"bytes"
	"encoding/binary"
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
	payload := []byte("m value=1 1")
	seq, _ := w.Append(protocol.TypeData, payload)
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
	// 帧内容必须可完整解析（回归：曾因读取偏移漏加 recordHeadLen 错位 4 字节）
	fb, err := os.ReadFile(framePath)
	if err != nil {
		t.Fatalf("read dlq frame: %v", err)
	}
	f, err := protocol.Decode(fb)
	if err != nil {
		t.Fatalf("dlq frame not decodable (offset bug?): %v", err)
	}
	if f.Seq != seq {
		t.Fatalf("dlq frame seq=%d want %d", f.Seq, seq)
	}
	raw, err := f.Decompress()
	if err != nil {
		t.Fatalf("dlq frame decompress: %v", err)
	}
	if !bytes.Equal(raw, payload) {
		t.Fatalf("dlq payload mismatch: %q vs %q", raw, payload)
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

// TestTornTailRecovery C1/P0：头完整+帧体撕裂（崩溃落在两次 Write 之间）
// 必须截断尾部恢复，而不是 Open 失败导致进程起不来。
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 两帧正常落盘
	seq1, err := w.Append(protocol.TypeData, []byte("m value=1 1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(protocol.TypeData, []byte("m value=2 2")); err != nil {
		t.Fatal(err)
	}
	w.SetCursor(1000)
	w.Close()

	// 模拟撕裂：追加一段 [u32 len=200][帧体前半截]（头完整、体不完整）
	f, err := os.OpenFile(segPath(walDir, 0), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], 200)
	f.Write(head[:])
	f.Write([]byte("partial frame body")) // 只有 18 字节
	f.Close()

	// 重开：必须成功，撕裂尾被截断，两帧完好
	w2, err := Open(walDir, 0)
	if err != nil {
		t.Fatalf("open after torn tail must succeed: %v", err)
	}
	defer w2.Close()
	if w2.PendingCount() != 2 {
		t.Fatalf("pending=%d, want 2 (torn tail truncated)", w2.PendingCount())
	}
	s, _, err := w2.Peek()
	if err != nil || s != seq1 {
		t.Fatalf("peek seq=%d err=%v", s, err)
	}
	// 截断后仍可继续追加
	seq3, err := w2.Append(protocol.TypeData, []byte("m value=3 3"))
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if seq3 != 3 {
		t.Fatalf("seq3=%d", seq3)
	}
	// 重启后索引一致
	w2.Close()
	w3, err := Open(walDir, 0)
	if err != nil {
		t.Fatalf("reopen after recovery: %v", err)
	}
	defer w3.Close()
	if w3.PendingCount() != 3 {
		t.Fatalf("pending=%d after reopen", w3.PendingCount())
	}
}

// TestTornHeadRecovery C1：头部分写入（1~3 字节）同样截断恢复。
func TestTornHeadRecovery(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(protocol.TypeData, []byte("m value=1 1")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	f, err := os.OpenFile(segPath(walDir, 0), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x00, 0x00}) // 2 字节撕裂头
	f.Close()

	w2, err := Open(walDir, 0)
	if err != nil {
		t.Fatalf("open after torn head must succeed: %v", err)
	}
	defer w2.Close()
	if w2.PendingCount() != 1 {
		t.Fatalf("pending=%d, want 1", w2.PendingCount())
	}
	seq, err := w2.Append(protocol.TypeData, []byte("m value=2 2"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seq != 2 {
		t.Fatalf("seq=%d", seq)
	}
}

// TestCheckpointThrottled P3：Commit 路径的 checkpoint 持久化节流到每秒一次，
// 段删除/SetCursor/Close 立即持久化。
func TestCheckpointThrottled(t *testing.T) {
	w := newTestWAL(t, 0)
	seqs := make([]uint64, 0, 5)
	for i := 0; i < 5; i++ {
		s, err := w.Append(protocol.TypeData, []byte("m value=1 1"))
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s)
	}
	w.SetCursor(100) // 立即持久化
	first := w.lastCp
	if first.IsZero() {
		t.Fatal("SetCursor must persist immediately")
	}
	// 同一秒内多次 Commit：不得再次持久化（lastCp 不变）
	for i := 0; i < 4; i++ {
		if err := w.Commit(seqs[i]); err != nil {
			t.Fatal(err)
		}
		if !w.lastCp.Equal(first) {
			t.Fatalf("commit %d persisted checkpoint (throttle broken)", i)
		}
	}
	// 关闭时最终持久化
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(w.dir, "checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.AckedBytes == 0 {
		t.Fatal("checkpoint must reflect commits on Close")
	}
}

// TestAppendBatchGroupCommit P4：多帧一次 fsync 落盘，重启后全部可恢复。
func TestAppendBatchGroupCommit(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	for i := 0; i < 10; i++ {
		fb, err := protocol.Encode(protocol.TypeData, 0, []byte(fmt.Sprintf("m value=%d %d", i, i))) // 占位 seq=0
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, fb)
	}
	seqs, err := w.AppendBatch(protocol.TypeData, frames)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if len(seqs) != 10 {
		t.Fatalf("seqs len=%d", len(seqs))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("seqs not consecutive: %v", seqs)
		}
	}
	if seqs[0] != 1 {
		t.Fatalf("first seq=%d, want 1 (fresh WAL starts at 1, N7)", seqs[0])
	}
	if w.PendingCount() != 10 {
		t.Fatalf("pending=%d", w.PendingCount())
	}
	if w.NextSeq() != 11 {
		t.Fatalf("next_seq=%d", w.NextSeq())
	}
	// 帧头 seq 已被重写为分配值
	fd0, _, err := w.Peek()
	if err != nil || fd0 != seqs[0] {
		t.Fatalf("peek seq=%d err=%v, want %d", fd0, err, seqs[0])
	}
	w.SetCursor(500)
	w.Close()

	w2, err := Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.PendingCount() != 10 {
		t.Fatalf("recovered pending=%d", w2.PendingCount())
	}
	fds, err := w2.PeekBatch(3)
	if err != nil || len(fds) != 3 {
		t.Fatalf("PeekBatch: %d %v", len(fds), err)
	}
	if fds[0].Seq != 1 || fds[2].Seq != 3 {
		t.Fatalf("peek seqs=%d..%d", fds[0].Seq, fds[2].Seq)
	}
	// 追加的第二批 seq 应从 11 连续续上（内部 seq 分配不受外部影响）
	if _, err := w2.AppendBatch(protocol.TypeData, frames[:1]); err != nil {
		t.Fatalf("second batch append: %v", err)
	}
	if w2.NextSeq() != 12 {
		t.Fatalf("next_seq=%d, want 12", w2.NextSeq())
	}
}

// TestMidSegmentCorruptionSkipsSingleRecord N3/P1：中段 bit-rot（记录头长度字段
// 损坏但后续帧完好）→ 只跳过坏帧并重同步，不截断整个尾部（丢一帧而非一尾）。
func TestMidSegmentCorruptionSkipsSingleRecord(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 三帧落盘
	seqs := make([]uint64, 0, 3)
	for i := 0; i < 3; i++ {
		s, err := w.Append(protocol.TypeData, []byte(fmt.Sprintf("m value=%d %d", i, i)))
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s)
	}
	w.SetCursor(100)
	w.Close()

	// 定位第 2 帧（seqs[1]）的记录头并破坏其长度字段
	segPath0 := segPath(walDir, 0)
	f, err := os.OpenFile(segPath0, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 帧 1 长度：read record head at 0
	var head [4]byte
	f.ReadAt(head[:], 0)
	off := int64(recordHeadLen) + int64(binary.BigEndian.Uint32(head[:]))
	// 破坏帧 2 的长度字段（bit-rot 模拟）
	f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, off)
	st, _ := f.Stat()
	sizeBefore := st.Size()
	f.Close()

	// 重开：必须成功；帧 2 被跳过，帧 1/3 完好；文件未被截断（帧 3 仍在）
	w2, err := Open(walDir, 0)
	if err != nil {
		t.Fatalf("open after mid-segment corruption: %v", err)
	}
	defer w2.Close()
	if w2.PendingCount() != 2 {
		t.Fatalf("pending=%d, want 2 (corrupt frame skipped, others kept)", w2.PendingCount())
	}
	s1, _, err := w2.Peek()
	if err != nil || s1 != seqs[0] {
		t.Fatalf("peek1 seq=%d err=%v", s1, err)
	}
	if err := w2.Commit(s1); err != nil {
		t.Fatal(err)
	}
	s3, _, err := w2.Peek()
	if err != nil || s3 != seqs[2] {
		t.Fatalf("peek2 seq=%d err=%v (frame after corruption must survive)", s3, err)
	}
	st2, _ := os.Stat(segPath0)
	if st2.Size() != sizeBefore {
		t.Fatalf("file truncated: %d -> %d (must skip single record, not whole tail)", sizeBefore, st2.Size())
	}
}
