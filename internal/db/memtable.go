package db

import (
	"bytes"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/arenaskl"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"sync"
)

const MB = 1024 * 1024

type memTable struct {
	skl      *arenaskl.Skiplist
	cfg      config.MemConfig
	maxKey   []byte
	minKey   []byte
	maxIndex uint64
	minIndex uint64
}

func newMemTable(cfg config.MemConfig) *memTable {
	arena := arenaskl.NewArena(uint32(cfg.MemTableSize*MB) + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	table := &memTable{cfg: cfg, skl: skl}
	return table
}

func (mt *memTable) newSklIter() *arenaskl.Iterator {
	sklIter := new(arenaskl.Iterator)
	sklIter.Init(mt.skl)
	return sklIter
}

func (mt *memTable) Put(kv *marshal.BytesKV) error {
	sklIter := mt.newSklIter()
	err := sklIter.Put(kv.Key, kv.Value)
	if err != nil {
		return err
	}
	return nil
}

func (mt *memTable) ConcurrentPut(kvBytes []*marshal.BytesKV) error {
	if len(kvBytes) == 0 {
		return nil
	}

	workers := mt.cfg.Concurrency
	if workers <= 1 || len(kvBytes) < workers*8 {
		return mt.putBatch(kvBytes)
	}
	if workers > len(kvBytes) {
		workers = len(kvBytes)
	}

	parts := make([][]*marshal.BytesKV, workers)
	for i, kv := range kvBytes {
		part := i % workers
		parts[part] = append(parts[part], kv)
	}

	errC := make(chan error, workers)
	wg := &sync.WaitGroup{}
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		wg.Add(1)
		go func(kvs []*marshal.BytesKV) {
			defer wg.Done()
			sklIter := mt.newSklIter()
			for _, kv := range kvs {
				err := sklIter.Put(kv.Key, kv.Value)
				if err != nil {
					select {
					case errC <- err:
					default:
					}
					return
				}
			}
		}(part)
	}
	wg.Wait()

	select {
	case err := <-errC:
		return err
	default:
	}
	return nil
}

func (mt *memTable) putBatch(kvBytes []*marshal.BytesKV) error {
	sklIter := mt.newSklIter()
	for _, kv := range kvBytes {
		if err := sklIter.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func (mt *memTable) Get(key []byte) (*marshal.KV, bool) {
	sklIter := mt.newSklIter()
	if found := sklIter.Seek(key); !found {
		return nil, false
	}
	value, _ := sklIter.Get(key)
	return &marshal.KV{Key: key, Data: marshal.DecodeData(value)}, true
}

func (mt *memTable) Scan(low, high []byte) (kvs []*marshal.KV, err error) {
	sklIter := mt.newSklIter()
	sklIter.Seek(low)

	for sklIter.Valid() {
		if len(high) > 0 && bytes.Compare(sklIter.Key(), high) >= 0 {
			break
		}
		key, value := sklIter.Key(), sklIter.Value()
		kvs = append(kvs, &marshal.KV{Key: key, KeySize: uint32(len(key)), Data: marshal.DecodeData(value)})
		sklIter.Next()
	}
	return
}

func (mt *memTable) All() (kvs []*marshal.BytesKV) {
	sklIter := mt.newSklIter()
	sklIter.SeekToFirst()
	for sklIter.Valid() {
		key, value := sklIter.Key(), sklIter.Value()
		kvs = append(kvs, &marshal.BytesKV{
			Key:   key,
			Value: value,
		})
		sklIter.Next()
	}
	return
}

func (mt *memTable) Size() int64 {
	return int64(mt.skl.Size())
}

type QueueNode struct {
	prev  *QueueNode
	next  *QueueNode
	value *memTable
}

type memTableQueue struct {
	front    *QueueNode
	rear     *QueueNode
	size     int
	capacity int
}

func newMemTableQueue(capacity int) *memTableQueue {
	mq := &memTableQueue{
		front:    new(QueueNode),
		rear:     new(QueueNode),
		size:     0,
		capacity: capacity,
	}
	mq.front.next = mq.rear
	mq.rear.prev = mq.front
	return mq
}

func (q *memTableQueue) Enqueue(item *memTable) {
	newNode := &QueueNode{value: item}
	q.rear.prev.next = newNode
	newNode.prev = q.rear.prev
	newNode.next = q.rear
	q.rear.prev = newNode
	q.size++
}

func (q *memTableQueue) Dequeue() *memTable {
	if q.size == 0 {
		return nil
	}
	node := q.front.next
	q.front.next.next.prev = q.front
	q.front.next = q.front.next.next
	node.prev = nil
	node.next = nil
	q.size--
	return node.value
}

func (q *memTableQueue) All() []*memTable {
	tables := make([]*memTable, 0)
	current := q.front.next
	for current != nil && current != q.rear {
		tables = append(tables, current.value)
		current = current.next
	}
	return tables
}
