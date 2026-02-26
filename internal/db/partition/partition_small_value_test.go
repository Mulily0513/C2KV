package partition

import (
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"sync"
	"testing"
	"time"
)

func persistPartitionKVs(t *testing.T, p *Partition, kvs []*marshal.KV) {
	t.Helper()
	errC := make(chan error, 1)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go p.PersistKvs(kvs, wg, errC)
	wg.Wait()
	select {
	case err := <-errC:
		t.Fatalf("persist kvs failed: %v", err)
	default:
	}
}

func TestPartitionGetSmallValueFromIndexer(t *testing.T) {
	dir := t.TempDir()
	p := OpenPartition(dir)
	t.Cleanup(func() { _ = p.Remove() })

	kv := &marshal.KV{
		Key: []byte("k-small"),
		Data: &marshal.Data{
			Index:     1,
			TimeStamp: time.Now().Unix(),
			Type:      marshal.TypeInsert,
			Value:     []byte("small-value"),
		},
	}
	persistPartitionKVs(t, p, []*marshal.KV{kv})

	got, err := p.Get(kv.Key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(got.Key) != string(kv.Key) || string(got.Data.Value) != string(kv.Data.Value) {
		t.Fatalf("unexpected kv, key=%q value=%q", string(got.Key), string(got.Data.Value))
	}
}

func TestPartitionScanContainsSmallValues(t *testing.T) {
	dir := t.TempDir()
	p := OpenPartition(dir)
	t.Cleanup(func() { _ = p.Remove() })

	kvs := []*marshal.KV{
		{
			Key: []byte("a"),
			Data: &marshal.Data{
				Index:     1,
				TimeStamp: time.Now().Unix(),
				Type:      marshal.TypeInsert,
				Value:     []byte("v1"),
			},
		},
		{
			Key: []byte("b"),
			Data: &marshal.Data{
				Index:     2,
				TimeStamp: time.Now().Unix(),
				Type:      marshal.TypeInsert,
				Value:     []byte("v2"),
			},
		},
	}
	persistPartitionKVs(t, p, kvs)

	scanned, err := p.Scan([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(scanned) != 2 {
		t.Fatalf("expected 2 kvs, got %d", len(scanned))
	}
}
