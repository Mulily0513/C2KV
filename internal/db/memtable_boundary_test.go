package db

import (
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"testing"
)

func testMemCfg() config.MemConfig {
	return config.MemConfig{
		MemTableSize: 64,
		Concurrency:  1,
	}
}

func putKV(t *testing.T, mt *memTable, key, value string, index uint64) {
	t.Helper()
	err := mt.Put(&marshal.BytesKV{
		Key: []byte(key),
		Value: marshal.EncodeData(&marshal.Data{
			Index: index,
			Type:  marshal.TypeInsert,
			Value: []byte(value),
		}),
	})
	if err != nil {
		t.Fatalf("put key=%s failed: %v", key, err)
	}
}

func TestMemTableScanHighExclusive(t *testing.T) {
	mt := newMemTable(testMemCfg())
	putKV(t, mt, "a", "va", 1)
	putKV(t, mt, "b", "vb", 2)
	putKV(t, mt, "c", "vc", 3)

	kvs, err := mt.Scan([]byte("b"), []byte("c"))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("expected 1 kv, got %d", len(kvs))
	}
	if string(kvs[0].Key) != "b" {
		t.Fatalf("expected key b, got %q", string(kvs[0].Key))
	}
}

func TestMemTableScanSeekMissReturnsEmpty(t *testing.T) {
	mt := newMemTable(testMemCfg())
	putKV(t, mt, "a", "va", 1)
	putKV(t, mt, "b", "vb", 2)

	kvs, err := mt.Scan([]byte("z"), nil)
	if err != nil {
		t.Fatalf("scan should not return error when seek misses, got %v", err)
	}
	if len(kvs) != 0 {
		t.Fatalf("expected empty kvs, got %d", len(kvs))
	}
}

func TestMemTableQueueAllSkipsSentinel(t *testing.T) {
	q := newMemTableQueue(4)
	m1 := newMemTable(testMemCfg())
	m2 := newMemTable(testMemCfg())
	q.Enqueue(m1)
	q.Enqueue(m2)

	all := q.All()
	if len(all) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(all))
	}
	for i, mt := range all {
		if mt == nil {
			t.Fatalf("table at index %d is nil", i)
		}
	}
}
