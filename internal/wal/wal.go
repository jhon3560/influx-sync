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
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/protocol"
)

const (
	DefaultSegmentSize int64 = 64 << 20 // 64MB
	DefaultDLQMaxSize  int64 = 1 << 30  // DLQ 目录上限 1GB
	recordHeadLen            = 4        // 段内记录长度头
	// checkpointInterval Commit 路径 checkpoint 持久化节流（P3）：
	// 每个 ACK 全量持久化（2 fsync+rename+目录 fsync）实测 5.4ms/次，
	// 重启时 scanSegments 重建索引、未确认帧重发由幂等覆盖，无需每次落盘。
	// SetCursor 保持每次持久化（先 WAL 后游标铁律不受影响）。
	checkpointInterval = time.Second
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

// FrameData 从 WAL 读出的待发送帧。
type FrameData struct {
	Seq   uint64
	Bytes []byte
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
	curOffset int64         // 当前写入段内偏移
	lockFile  *os.File      // 目录锁（防多实例并发）
	lastCp    time.Time     // 上次 checkpoint 持久化时间（Commit 节流用）
	notify    chan struct{} // 新增帧通知（cap=1，非阻塞），供 Sender 空闲唤醒
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
	w := &WAL{dir: dir, dlqDir: filepath.Join(filepath.Dir(dir), "dlq"), segSize: segSize, dlqMax: DefaultDLQMaxSize, lockFile: lockFile, notify: make(chan struct{}, 1)}

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
//
// 尾部撕裂恢复（C1/P0）：追加写是严格顺序的（头与体两次 Write + O_APPEND），
// 崩溃只会撕裂最后一条记录（头部分写入 / 头完整但体不完整）。扫描到首个
// 无效记录且其后无合法记录（重同步失败）时视为撕裂尾：截断到记录起点并记
// 日志，而不是整体失败导致进程起不来。
//
// 中段损坏（N3/P1）：bit-rot 等造成的坏记录如果后面还有合法帧，只跳过
// 单个坏记录并向前重新同步（丢一帧而非一尾，最多 64MB）；截断只在真尾部
// 损坏（重同步找不到下一个合法帧头）时发生。
func (w *WAL) indexSegment(idx int, path string, skip int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("wal: open segment %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat segment %s: %w", path, err)
	}
	fileSize := st.Size()
	buf := make([]byte, recordHeadLen+protocol.HeaderSize)
	var off int64
	// skipCorrupt：遇到无效记录时先尝试重同步；找不到后续合法记录才截断（撕裂尾）。
	// 返回新的扫描起点；返回 -1 表示无合法后续（调用方截断）。
	skipCorrupt := func(badOff int64, detail string) (int64, error) {
		if next := resyncRecord(f, fileSize, badOff); next >= 0 {
			zap.L().Warn("wal: skipping corrupt record in segment",
				zap.String("segment", path), zap.Int64("offset", badOff),
				zap.Int64("resync_to", next), zap.Int64("skipped_bytes", next-badOff),
				zap.String("detail", detail))
			return next, nil
		}
		if badOff < skip {
			// 无效记录位于已确认区域内（checkpoint 与文件不一致的极端损坏）
			return -1, fmt.Errorf("wal: corrupt segment %s at offset %d (%s) inside acked prefix, refuse to guess", path, badOff, detail)
		}
		if err := f.Truncate(badOff); err != nil {
			return -1, fmt.Errorf("wal: truncate torn tail of %s at %d: %w", path, badOff, err)
		}
		zap.L().Error("wal: truncated torn tail record",
			zap.String("segment", path), zap.Int64("offset", badOff),
			zap.Int64("dropped_bytes", fileSize-badOff), zap.String("detail", detail))
		return -1, nil
	}
	for {
		n, err := f.ReadAt(buf[:recordHeadLen], off)
		if err != nil {
			if n > 0 {
				// 头部分写入（1~3 字节）：尾部撕裂，截断后结束（后续不可能有合法帧）
				if _, terr := skipCorrupt(off, "torn record head"); terr != nil {
					return terr
				}
			}
			break // EOF：正常结束
		}
		length := int(binary.BigEndian.Uint32(buf[:recordHeadLen]))
		if length <= 0 || length > protocol.MaxFrameLen {
			next, terr := skipCorrupt(off, fmt.Sprintf("bad length %d", length))
			if terr != nil {
				return terr
			}
			if next < 0 {
				break
			}
			off = next
			continue
		}
		if _, err := f.ReadAt(buf[recordHeadLen:], off+recordHeadLen); err != nil {
			// 头完整 + 帧体撕裂（崩溃落在两次 Write 之间）→ 截断尾部恢复
			if _, terr := skipCorrupt(off, "torn frame body"); terr != nil {
				return terr
			}
			break
		}
		hdr, err := protocol.ParseHeader(buf[recordHeadLen:])
		// 记录长度必须与帧头 Length 一致（[u32 len] = HeaderSize + payload）——
		// 不一致说明记录头损坏（bit-rot），否则按错长度扫描会错位吞掉后续全部帧
		if err != nil || int64(length) != int64(protocol.HeaderSize)+int64(hdr.Length) {
			detail := fmt.Sprintf("bad frame header: %v (recLen=%d hdrLen=%d)", err, length, hdr.Length)
			next, terr := skipCorrupt(off, detail)
			if terr != nil {
				return terr
			}
			if next < 0 {
				break
			}
			off = next
			continue
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

// resyncRecord 从 badOff+1 起向前扫描，寻找下一个合法记录起点：
// [u32 len] 合法（0<len≤MaxFrameLen）+ 帧头可解析（魔数/版本/长度）+ 记录不越过 EOF。
// 扫描上限为 MaxFrameLen+头（下一合法记录必在坏记录真实末尾之后，而真实帧长≤MaxFrameLen）。
// 找不到返回 -1（视为尾部损坏，由调用方截断）。
func resyncRecord(f *os.File, fileSize, badOff int64) int64 {
	limit := badOff + int64(protocol.MaxFrameLen+recordHeadLen)
	if limit > fileSize {
		limit = fileSize
	}
	var head [recordHeadLen + protocol.HeaderSize]byte
	for p := badOff + 1; p+int64(recordHeadLen+protocol.HeaderSize) <= limit; p++ {
		if _, err := f.ReadAt(head[:recordHeadLen], p); err != nil {
			break
		}
		length := int(binary.BigEndian.Uint32(head[:recordHeadLen]))
		if length <= 0 || length > protocol.MaxFrameLen {
			continue
		}
		if p+recordHeadLen+int64(length) > fileSize {
			continue
		}
		if _, err := f.ReadAt(head[recordHeadLen:], p+recordHeadLen); err != nil {
			break
		}
		hdr, err := protocol.ParseHeader(head[recordHeadLen:])
		if err != nil {
			continue
		}
		// 长度一致性：记录头 len 必须等于 HeaderSize + 帧头 Length
		if int64(length) != int64(protocol.HeaderSize)+int64(hdr.Length) {
			continue
		}
		return p
	}
	return -1
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
// 单帧路径：每帧 fsync（中继 WAL 等低频场景的持久性保证）。
// 高频批量路径请用 AppendBatch（group commit）。
func (w *WAL) AppendEncoded(typ uint8, seq uint64, frameBytes []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendEncodedLocked(typ, seq, frameBytes)
}

func (w *WAL) appendEncodedLocked(typ uint8, seq uint64, frameBytes []byte) error {
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
	w.notifyAppend()
	return nil
}

// AppendBatch 一次追加多帧并在最后统一 fsync（group commit，P4）。
// seqBase 必须等于 NextSeq，帧 seq 依次为 seqBase+i。
// 每轮 poll 的所有帧只 fsync 一次：游标本就在 append 之后才推进，
// 崩溃时未 fsync 的尾部会因游标回退而重新查询，正确性不降。
func (w *WAL) AppendBatch(typ uint8, seqBase uint64, frameBytes [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(frameBytes) == 0 {
		return nil
	}
	if seqBase != w.cp.NextSeq {
		return fmt.Errorf("wal: out-of-order append seq=%d next=%d", seqBase, w.cp.NextSeq)
	}
	for i, fb := range frameBytes {
		if err := w.ensureSpace(len(fb) + recordHeadLen); err != nil {
			return err
		}
		var head [recordHeadLen]byte
		binary.BigEndian.PutUint32(head[:], uint32(len(fb)))
		if _, err := w.curFile.Write(head[:]); err != nil {
			return fmt.Errorf("wal: write record head: %w", err)
		}
		if _, err := w.curFile.Write(fb); err != nil {
			return fmt.Errorf("wal: write frame: %w", err)
		}
		w.index = append(w.index, frameIndex{
			seg: w.curSeg, offset: w.curOffset, length: len(fb), seq: seqBase + uint64(i), typ: typ,
		})
		w.curOffset += recordHeadLen + int64(len(fb))
		w.cp.NextSeq++
	}
	if err := w.curFile.Sync(); err != nil {
		return fmt.Errorf("wal: fsync batch: %w", err)
	}
	w.notifyAppend()
	return nil
}

// notifyAppend 非阻塞通知新帧到达（唤醒空闲 Sender，替代轮询 IdleSleep）。
func (w *WAL) notifyAppend() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// NotifyCh 返回新增帧通知通道（Sender 空闲等待用）。
func (w *WAL) NotifyCh() <-chan struct{} { return w.notify }

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
	if err := w.appendEncodedLocked(typ, seq, frameBytes); err != nil {
		return 0, err
	}
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
	fds, err := w.PeekBatch(1)
	if err != nil {
		return 0, nil, err
	}
	return fds[0].Seq, fds[0].Bytes, nil
}

// PeekBatch 返回最多 n 个最老未确认帧（按 seq 升序）。无帧时返回 ErrEmpty。
func (w *WAL) PeekBatch(n int) ([]FrameData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.acked >= len(w.index) {
		return nil, ErrEmpty
	}
	cnt := len(w.index) - w.acked
	if n > 0 && cnt > n {
		cnt = n
	}
	out := make([]FrameData, 0, cnt)
	for i := 0; i < cnt; i++ {
		fi := w.index[w.acked+i]
		buf := make([]byte, fi.length)
		readOff := fi.offset + recordHeadLen // 跳过 [u32 len] 记录头
		if fi.seg == w.curSeg {
			if _, err := w.curFile.ReadAt(buf, readOff); err != nil {
				return nil, fmt.Errorf("wal: read frame %d: %w", fi.seq, err)
			}
		} else {
			f, err := os.Open(segPath(w.dir, fi.seg))
			if err != nil {
				return nil, fmt.Errorf("wal: open seg for frame %d: %w", fi.seq, err)
			}
			_, rerr := f.ReadAt(buf, readOff)
			f.Close()
			if rerr != nil {
				return nil, fmt.Errorf("wal: read frame %d: %w", fi.seq, rerr)
			}
		}
		out = append(out, FrameData{Seq: fi.seq, Bytes: buf})
	}
	return out, nil
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
	segRemoved := false
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
		segRemoved = true
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
	// P3：段删除是目录级变化，立即持久化；普通 ACK 节流到每秒一次。
	// （scanSegments 能容忍 checkpoint 落后：缺失段自动从现存最老段重建索引）
	if segRemoved {
		return w.persistLocked()
	}
	return w.persistLockedThrottled()
}

// persistLocked 持久化 checkpoint（调用方持锁）。
func (w *WAL) persistLocked() error {
	if err := saveCheckpoint(w.dir, w.cp); err != nil {
		return err
	}
	w.lastCp = time.Now()
	return nil
}

// persistLockedThrottled Commit 路径的节流持久化：每秒最多一次。
func (w *WAL) persistLockedThrottled() error {
	if time.Since(w.lastCp) < checkpointInterval {
		return nil
	}
	return w.persistLocked()
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
	// fi.offset 是 [u32 len] 记录头起点，帧字节在 offset+recordHeadLen 处
	if _, err := f.ReadAt(frameBytes, fi.offset+recordHeadLen); err != nil {
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

// Close 关闭当前段文件并释放目录锁；退出前持久化最终 checkpoint
// （减少重启后未确认帧重发量）。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if w.curFile != nil {
		err = w.curFile.Close()
		w.curFile = nil
	}
	if perr := w.persistLocked(); err == nil && perr != nil {
		err = perr
	}
	if w.lockFile != nil {
		syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
		w.lockFile.Close()
		w.lockFile = nil
	}
	return err
}
