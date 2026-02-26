package partition

import (
	"bytes"
	"testing"
	"time"

	"github.com/Mulily0513/C2KV/internal/db/marshal"
)

func TestPartitionCompactionRewritesLargeValues(t *testing.T) {
	p := OpenPartition(t.TempDir())
	t.Cleanup(func() {
		_ = p.Close()
		_ = p.Remove()
	})

	var latestV1 []byte
	var latestV2 []byte
	for i := 0; i < compactionMinSST+2; i++ {
		latestV1 = bytes.Repeat([]byte{byte('a' + i%26)}, 256)
		latestV2 = bytes.Repeat([]byte{byte('A' + i%26)}, 320)
		persistPartitionKVs(t, p, []*marshal.KV{
			{
				Key: []byte("k1"),
				Data: &marshal.Data{
					Index:     uint64(i*2 + 1),
					TimeStamp: time.Now().UnixNano(),
					Type:      marshal.TypeInsert,
					Value:     latestV1,
				},
			},
			{
				Key: []byte("k2"),
				Data: &marshal.Data{
					Index:     uint64(i*2 + 2),
					TimeStamp: time.Now().UnixNano(),
					Type:      marshal.TypeInsert,
					Value:     latestV2,
				},
			},
		})
	}

	p.mu.RLock()
	before := len(p.SSTMap)
	p.mu.RUnlock()
	if before < compactionMinSST {
		t.Fatalf("expected enough sst files before compaction, got=%d", before)
	}

	p.mu.Lock()
	p.lastWriteAt = time.Now().Add(-time.Hour)
	p.mu.Unlock()
	p.Compaction()

	p.mu.RLock()
	after := len(p.SSTMap)
	p.mu.RUnlock()
	if after != 1 {
		t.Fatalf("expected one compacted sst, got=%d", after)
	}

	kv1, err := p.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1 failed after compaction: %v", err)
	}
	if !bytes.Equal(kv1.Data.Value, latestV1) {
		t.Fatalf("k1 value mismatch after compaction")
	}

	kv2, err := p.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2 failed after compaction: %v", err)
	}
	if !bytes.Equal(kv2.Data.Value, latestV2) {
		t.Fatalf("k2 value mismatch after compaction")
	}
}

func TestPartitionReopenKeepsSSTIDMapping(t *testing.T) {
	dir := t.TempDir()
	p := OpenPartition(dir)
	kv := &marshal.KV{
		Key: []byte("persist-key"),
		Data: &marshal.Data{
			Index:     1,
			TimeStamp: time.Now().UnixNano(),
			Type:      marshal.TypeInsert,
			Value:     bytes.Repeat([]byte("v"), 256),
		},
	}
	persistPartitionKVs(t, p, []*marshal.KV{kv})
	if err := p.Close(); err != nil {
		t.Fatalf("close partition failed: %v", err)
	}

	reopened := OpenPartition(dir)
	t.Cleanup(func() {
		_ = reopened.Close()
		_ = reopened.Remove()
	})
	got, err := reopened.Get(kv.Key)
	if err != nil {
		t.Fatalf("get after reopen failed: %v", err)
	}
	if !bytes.Equal(got.Data.Value, kv.Data.Value) {
		t.Fatalf("value mismatch after reopen")
	}
}
