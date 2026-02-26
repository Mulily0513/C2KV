package db

import (
	"bytes"
	"container/heap"

	"github.com/Mulily0513/C2KV/internal/db/marshal"
)

type kvMergeCursor struct {
	items []*marshal.KV
	idx   int
}

func (c *kvMergeCursor) current() *marshal.KV {
	if c == nil || c.idx < 0 || c.idx >= len(c.items) {
		return nil
	}
	return c.items[c.idx]
}

func (c *kvMergeCursor) advanceToNextValid() bool {
	for c.idx < len(c.items) {
		if kvMergeItemValid(c.items[c.idx]) {
			return true
		}
		c.idx++
	}
	return false
}

type kvMergeHeap []*kvMergeCursor

func (h kvMergeHeap) Len() int { return len(h) }

func (h kvMergeHeap) Less(i, j int) bool {
	ki := h[i].current().Key
	kj := h[j].current().Key
	return bytes.Compare(ki, kj) < 0
}

func (h kvMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *kvMergeHeap) Push(x any) {
	*h = append(*h, x.(*kvMergeCursor))
}

func (h *kvMergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func kvMergeItemValid(kv *marshal.KV) bool {
	return kv != nil && kv.Data != nil
}

func kvIsNewer(a, b *marshal.KV) bool {
	if b == nil || b.Data == nil {
		return true
	}
	if a == nil || a.Data == nil {
		return false
	}
	if a.Data.Index != b.Data.Index {
		return a.Data.Index > b.Data.Index
	}
	return a.Data.TimeStamp > b.Data.TimeStamp
}

func mergeSortedKVSources(sources [][]*marshal.KV, dedupLatest bool) []*marshal.KV {
	estimated := 0
	h := make(kvMergeHeap, 0, len(sources))
	for _, source := range sources {
		if len(source) == 0 {
			continue
		}
		estimated += len(source)
		cursor := &kvMergeCursor{items: source}
		if cursor.advanceToNextValid() {
			h = append(h, cursor)
		}
	}
	if len(h) == 0 {
		return nil
	}
	heap.Init(&h)

	result := make([]*marshal.KV, 0, estimated)
	if !dedupLatest {
		for h.Len() > 0 {
			cursor := heap.Pop(&h).(*kvMergeCursor)
			result = append(result, cursor.current())
			cursor.idx++
			if cursor.advanceToNextValid() {
				heap.Push(&h, cursor)
			}
		}
		return result
	}

	for h.Len() > 0 {
		cursor := heap.Pop(&h).(*kvMergeCursor)
		current := cursor.current()
		key := current.Key
		best := current
		group := []*kvMergeCursor{cursor}

		for h.Len() > 0 {
			top := h[0].current()
			if !bytes.Equal(top.Key, key) {
				break
			}
			next := heap.Pop(&h).(*kvMergeCursor)
			group = append(group, next)
			if kvIsNewer(next.current(), best) {
				best = next.current()
			}
		}

		result = append(result, best)

		for _, c := range group {
			c.idx++
			if c.advanceToNextValid() {
				heap.Push(&h, c)
			}
		}
	}
	return result
}
