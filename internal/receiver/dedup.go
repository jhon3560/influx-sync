// Package receiver 实现 III 区接收端：校验 → 去重 → 批量写 → ACK。
package receiver

import (
	"container/list"
	"sync"
)

// LRU seq 去重缓存：最近处理过的帧序号（容量上限淘汰最旧）。
// 依据 AGENTS.md：Receiver 去重使用内存 LRU，不用 SQLite。
type LRU struct {
	mu   sync.Mutex
	cap  int
	m    map[uint64]*list.Element
	list *list.List // 元素为 uint64
}

// NewLRU 创建容量 cap 的 LRU（cap<=0 时默认 10000）。
func NewLRU(cap int) *LRU {
	if cap <= 0 {
		cap = 10000
	}
	return &LRU{cap: cap, m: make(map[uint64]*list.Element), list: list.New()}
}

// CheckAndAdd 若 seq 已存在返回 true（重复）；否则插入并返回 false。
func (l *LRU) CheckAndAdd(seq uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.m[seq]; ok {
		l.list.MoveToFront(e)
		return true
	}
	e := l.list.PushFront(seq)
	l.m[seq] = e
	for l.list.Len() > l.cap {
		back := l.list.Back()
		if back == nil {
			break
		}
		l.list.Remove(back)
		delete(l.m, back.Value.(uint64))
	}
	return false
}

// Contains 判断 seq 是否存在（不改变顺序）。
func (l *LRU) Contains(seq uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.m[seq]
	return ok
}

// Len 返回当前缓存数量。
func (l *LRU) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.list.Len()
}
