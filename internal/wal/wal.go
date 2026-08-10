// Package wal 实现 Segment WAL（预写日志），保证"数据不丢"。
//
// 设计（依据 AGENTS.md §4/§8）：
//   - 追加写大文件（默认 64MB/段），禁止一帧一文件（防 inode 爆炸）
//   - 段内记录格式：[u32 len][frame bytes]，frame 为 protocol.Encode 输出
//   - 帧顺序发送（Stop-And-Wait），确认按 seq 推进
//   - checkpoint 原子持久化：cursor、next_seq、段确认位置
//   - 段内全部确认后整段删除
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"influx-sync/internal/protocol"
)

const (
	DefaultSegmentSize int64 = 64 << 20 // 64MB
	DefaultDLQMaxSize  int64 = 1 << 30  // DLQ 目录上限 1GB
	recordHeadLen            = 4        // 段内记录长度头
)

// ErrEmpty WAL 无待发送帧。
var ErrEmpty = errors.New("wal: empty")

// frameIndex 内存中的帧索引（启动时扫描段文件重建）。
type frameIndex struct {
	seg    int   // 所属段序号
	offset int64 // 段内偏移（记录头起点）
	length int   // 帧字节数（含 Header）
	seq    uint64
	typ    uint8
}

// WAL 分段追加写日志。
type WAL struct {
	mu        sync.Mutex
	dir       string
	dlqDir    string
	segSize   int64
	dlqMax    int64
	cp        checkpoint
	index     []frameIndex // 按 seg/offset 排序，仅含未确认帧
	acked     int          // index 中已确认前缀的长度
	curSeg    int          // 当前写入段序号
	curFile   *os.File
	curOffset int64    // 当前写入段内偏移
	lockFile  *os.File // 目录锁（防多实例并发）
}

// Open 打开（或创建）WAL。segSize<=0 时用默认 64MB。
// 通过目录锁文件防止多实例同时操作同一 WAL（数据损坏防护）。
func Open(dir string, segSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}
	lockFile, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("wal: directory locked by another process (flock): %w", err)
	}
	if segSize <= 0 {
		segSize = DefaultSegmentSize
	}
	w := &WAL{dir: dir, dlqDir: filepath.Join(filepath.Dir(dir), "dlq"), segSize: segSize, dlqMax: DefaultDLQMaxSize, lockFile: lockFile}

	cp, err := loadCheckpoint(dir)
	if err != nil {
		return nil, err
	}
	w.cp = cp

	if err := w.scanSegments(); err != nil {
		return nil, err
	}
	// 恢复 next_seq：不得小于 checkpoint 与已扫描帧最大值
	var maxSeq uint64
	for _, fi := range w.index {
		if fi.seq > maxSeq {
			maxSeq = fi.seq
		}
	}
	if w.cp.NextSeq < maxSeq+1 {
		w.cp.NextSeq = maxSeq + 1
	}
	// 打开当前写入段（追加模式）
	curIdx, err := w.lastSegmentIdx()
	if err != nil {
		return nil, err
	}
	w.curSeg = curIdx
	f, err := os.OpenFile(segPath(dir, curIdx), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open current segment: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat current segment: %w", err)
	}
	w.curFile = f
	w.curOffset = st.Size()
	return w, nil
}

func segPath(dir string, idx int) string {
	return filepath.Join(dir, fmt.Sprintf("seg-%06d.log", idx))
}

func parseSegIdx(name string) (int, bool) {
	if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".log"))
	if err != nil {
		return 0, false
	}
	return n, true
}

// scanSegments 扫描目录，删除 checkpoint 之前的段，重建未确认帧索引。
func (w *WAL) scanSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("wal: read dir: %w", err)
	}
	segs := map[int]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if idx, ok := parseSegIdx(e.Name()); ok {
			segs[idx] = filepath.Join(w.dir, e.Name())
		}
	}
	idxList := make([]int, 0, len(segs))
	for idx := range segs {
		idxList = append(idxList, idx)
	}
	sort.Ints(idxList)

	// 删除 checkpoint 之前已确认的段
	firstAlive := -1
	for _, idx := range idxList {
		if idx < w.cp.SegStart {
			if err := os.Remove(segs[idx]); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal: remove stale segment: %w", err)
			}
			continue
		}
		if firstAlive < 0 {
			firstAlive = idx
		}
	}
	if firstAlive < 0 {
		// 全部已确认或没有段：新建 0 号段
		if w.cp.SegStart < 0 {
			w.cp.SegStart = 0
		}
		w.cp.AckedBytes = 0
		return nil
	}
	// 若 checkpoint 指向的段已被删除，从现存最老段开始
	if firstAlive > w.cp.SegStart {
		w.cp.SegStart = firstAlive
		w.cp.AckedBytes = 0
	}
	// 重建索引：从 (SegStart, AckedBytes) 开始
	for _, idx := range idxList {
		if idx < w.cp.SegStart {
			continue
		}
		skip := int64(0)
		if idx == w.cp.SegStart {
			skip = w.cp.AckedBytes
		}
		if err := w.indexSegment(idx, segs[idx], skip); err != nil {
			return err
		}
	}
	w.acked = 0
	return nil
}

// indexSegment 扫描单个段文件，建立帧索引；offset<skip 的帧视为已确认跳过。
func (w *WAL) indexSegment(idx int, path string, skip int64) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal: open segment %s: %w", path, err)
	}
	defer f.Close()
	buf := make([]byte, recordHeadLen+protocol.HeaderSize)
	var off int64
	for {
		if _, err := f.ReadAt(buf[:recordHeadLen], off); err != nil {
			break // EOF
		}
		length := int(binary.BigEndian.Uint32(buf[:recordHeadLen]))
		if length <= 0 || length > protocol.MaxFrameLen {
			return fmt.Errorf("wal: corrupt segment %s at offset %d: bad length %d", path, off, length)
		}
		if _, err := f.ReadAt(buf[recordHeadLen:], off+recordHeadLen); err != nil {
			return fmt.Errorf("wal: corrupt segment %s at offset %d: %w", path, off, err)
		}
		hdr, err := protocol.ParseHeader(buf[recordHeadLen:])
		if err != nil {
			return fmt.Errorf("wal: corrupt segment %s at offset %d: %w", path, off, err)
		}
		if off+recordHeadLen+int64(length) <= skip {
			// 已确认
		} else {
			w.index = append(w.index, frameIndex{
				seg: idx, offset: off, length: length, seq: hdr.Seq, typ: hdr.Type,
			})
		}
		off += recordHeadLen + int64(length)
	}
	return nil
}

func (w *WAL) lastSegmentIdx() (int, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, fmt.Errorf("wal: read dir: %w", err)
	}
	last := w.cp.SegStart
	found := false
	for _, e := range entries {
		if idx, ok := parseSegIdx(e.Name()); ok {
			if !found || idx > last {
				last = idx
				found = true
			}
		}
	}
	if !found {
		last = w.cp.SegStart
	}
	if last < 0 {
		last = 0
	}
	return last, nil
}

// AppendEncoded 追加一帧已编码的帧字节（编码由调用方完成，锁内只做 IO）。
// 调用方必须保证 seq 与内部 NextSeq 严格递增一致（顺序铁律），否则返回错误。
func (w *WAL) AppendEncoded(typ uint8, seq uint64, frameBytes []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if seq != w.cp.NextSeq {
		return fmt.Errorf("wal: out-of-order append seq=%d next=%d", seq, w.cp.NextSeq)
	}
	if err := w.ensureSpace(len(frameBytes) + recordHeadLen); err != nil {
		return err
	}
	var head [recordHeadLen]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(frameBytes)))
	if _, err := w.curFile.Write(head[:]); err != nil {
		return fmt.Errorf("wal: write record head: %w", err)
	}
	if _, err := w.curFile.Write(frameBytes); err != nil {
		return fmt.Errorf("wal: write frame: %w", err)
	}
	if err := w.curFile.Sync(); err != nil {
		return fmt.Errorf("wal: fsync frame: %w", err)
	}
	w.index = append(w.index, frameIndex{
		seg: w.curSeg, offset: w.curOffset, length: len(frameBytes), seq: seq, typ: typ,
	})
	w.curOffset += recordHeadLen + int64(len(frameBytes))
	w.cp.NextSeq = seq + 1
	return nil
}

// Append 追加一帧（内部分配 seq 并编码），成功后 fsync。
// 返回该帧的 seq。调用方必须在成功后推进游标（SetCursor）。
func (w *WAL) Append(typ uint8, payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.cp.NextSeq
	frameBytes, err := protocol.Encode(typ, seq, payload)
	if err != nil {
		return 0, fmt.Errorf("wal: encode: %w", err)
	}
	if err := w.ensureSpace(len(frameBytes) + recordHeadLen); err != nil {
		return 0, err
	}
	var head [recordHeadLen]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(frameBytes)))
	if _, err := w.curFile.Write(head[:]); err != nil {
		return 0, fmt.Errorf("wal: write record head: %w", err)
	}
	if _, err := w.curFile.Write(frameBytes); err != nil {
		return 0, fmt.Errorf("wal: write frame: %w", err)
	}
	if err := w.curFile.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync frame: %w", err)
	}
	// 帧落盘成功后才推进内存状态
	w.index = append(w.index, frameIndex{
		seg: w.curSeg, offset: w.curOffset, length: len(frameBytes), seq: seq, typ: typ,
	})
	w.curOffset += recordHeadLen + int64(len(frameBytes))
	w.cp.NextSeq = seq + 1
	return seq, nil
}

// ensureSpace 检查当前段剩余空间，不足或未打开则滚动/创建新段。
func (w *WAL) ensureSpace(need int) error {
	if w.curFile == nil {
		return w.rotate()
	}
	if w.curOffset+int64(need) <= w.segSize {
		return nil
	}
	if err := w.curFile.Close(); err != nil {
		return fmt.Errorf("wal: close segment: %w", err)
	}
	w.curSeg++
	return w.rotate()
}

// rotate 创建（或重建）当前段文件。
func (w *WAL) rotate() error {
	f, err := os.OpenFile(segPath(w.dir, w.curSeg), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: stat segment: %w", err)
	}
	w.curFile = f
	w.curOffset = st.Size()
	return nil
}

// Peek 返回最老未确认帧。无帧时返回 ErrEmpty。
func (w *WAL) Peek() (seq uint64, frameBytes []byte, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.acked >= len(w.index) {
		return 0, nil, ErrEmpty
	}
	fi := w.index[w.acked]
	buf := make([]byte, fi.length)
	readOff := fi.offset + recordHeadLen // 跳过 [u32 len] 记录头
	if fi.seg == w.curSeg {
		if _, err := w.curFile.ReadAt(buf, readOff); err != nil {
			return 0, nil, fmt.Errorf("wal: read frame %d: %w", fi.seq, err)
		}
	} else {
		f, err := os.Open(segPath(w.dir, fi.seg))
		if err != nil {
			return 0, nil, fmt.Errorf("wal: open seg for frame %d: %w", fi.seq, err)
		}
		defer f.Close()
		if _, err := f.ReadAt(buf, readOff); err != nil {
			return 0, nil, fmt.Errorf("wal: read frame %d: %w", fi.seq, err)
		}
	}
	return fi.seq, buf, nil
}

// Commit 确认帧 seq。Stop-And-Wait 下应顺序确认；非顺序时仅推进前缀。
func (w *WAL) Commit(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.commitLocked(seq)
}

func (w *WAL) commitLocked(seq uint64) error {
	if w.acked >= len(w.index) || w.index[w.acked].seq != seq {
		return fmt.Errorf("wal: commit out-of-order seq %d (pending %d)", seq, w.index[w.acked].seq)
	}
	cur := w.index[w.acked]
	w.acked++
	w.cp.AckedBytes = cur.offset + recordHeadLen + int64(cur.length)
	// 最老段全部确认后整段删除并前进
	for {
		segDone := w.acked >= len(w.index) || w.index[w.acked].seg != w.cp.SegStart
		if !segDone {
			break
		}
		removed := w.cp.SegStart
		if err := os.Remove(segPath(w.dir, removed)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: remove segment: %w", err)
		}
		w.cp.SegStart++
		w.cp.AckedBytes = 0
		w.index = w.index[w.acked:]
		w.acked = 0
		// 删除的恰是当前写入段时，滚动到新段序号（文件延迟到下次 Append 创建）
		if removed == w.curSeg {
			if err := w.curFile.Close(); err != nil {
				return fmt.Errorf("wal: close rotated segment: %w", err)
			}
			w.curFile = nil
			w.curSeg++
			w.curOffset = 0
		}
		if len(w.index) == 0 {
			break
		}
	}
	return w.persistLocked()
}

// persistLocked 持久化 checkpoint（调用方持锁）。
func (w *WAL) persistLocked() error {
	return saveCheckpoint(w.dir, w.cp)
}

// SetCursor 更新逻辑游标并持久化。
// 调用前提：对应数据已成功 Append（先 WAL 后游标，违反会漏数据）。
func (w *WAL) SetCursor(ts int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ts < w.cp.CursorNs {
		return fmt.Errorf("wal: cursor regress %d -> %d", w.cp.CursorNs, ts)
	}
	w.cp.CursorNs = ts
	return w.persistLocked()
}

// Cursor 返回当前逻辑游标。
func (w *WAL) Cursor() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cp.CursorNs
}

// NextSeq 返回下一个帧序号。
func (w *WAL) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cp.NextSeq
}

// PendingCount 返回未确认帧数。
func (w *WAL) PendingCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.index) - w.acked
}

// PendingBytes 返回未确认帧总字节数。
func (w *WAL) PendingBytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var n int64
	for _, fi := range w.index[w.acked:] {
		n += recordHeadLen + int64(fi.length)
	}
	return n
}

// DiskUsage 返回 WAL 目录占用字节数（含段文件与 checkpoint）。
func (w *WAL) DiskUsage() int64 {
	var n int64
	entries, err := os.ReadDir(w.dir)
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

// MoveToDLQ 将帧转存死信目录并从 WAL 移除（防止毒丸卡死主链路）。
func (w *WAL) MoveToDLQ(seq uint64, reason string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.acked >= len(w.index) || w.index[w.acked].seq != seq {
		return fmt.Errorf("wal: dlq unknown seq %d", seq)
	}
	fi := w.index[w.acked]
	frameBytes := make([]byte, fi.length)
	p := segPath(w.dir, fi.seg)
	f, err := os.Open(p)
	if err != nil {
		return fmt.Errorf("wal: open seg for dlq: %w", err)
	}
	if _, err := f.ReadAt(frameBytes, fi.offset); err != nil {
		f.Close()
		return fmt.Errorf("wal: read frame for dlq: %w", err)
	}
	f.Close()

	dlqDir := w.dlqDir
	if dlqSize(dlqDir) > w.dlqMax {
		return fmt.Errorf("wal: dlq over capacity: %d bytes > %d", dlqSize(dlqDir), w.dlqMax)
	}
	if err := os.MkdirAll(dlqDir, 0o755); err != nil {
		return fmt.Errorf("wal: mkdir dlq: %w", err)
	}
	if err := writeDLQ(dlqDir, seq, frameBytes, reason); err != nil {
		return err
	}
	return w.commitLocked(seq)
}

// Dir 返回 WAL 目录。
func (w *WAL) Dir() string { return w.dir }

// Close 关闭当前段文件并释放目录锁。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if w.curFile != nil {
		err = w.curFile.Close()
		w.curFile = nil
	}
	if w.lockFile != nil {
		syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
		w.lockFile.Close()
		w.lockFile = nil
	}
	return err
}
