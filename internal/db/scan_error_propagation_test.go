package db

import (
	"bytes"
	"fmt"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"path/filepath"
	"testing"
	"time"
)

func TestC2KVScanReturnsValueLogError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.ValueLogConfig{
		ValueLogDir:   dir,
		PartitionNums: 2,
	}
	stateSeg, err := wal.OpenVlogStateSegment(filepath.Join(t.TempDir(), "state"+wal.VlogSuffix))
	if err != nil {
		t.Fatalf("open vlog state segment failed: %v", err)
	}
	vlog := openValueLog(cfg, make(chan *memTable, 1), stateSeg)
	t.Cleanup(func() {
		_ = vlog.Delete()
	})

	largeValue := bytes.Repeat([]byte("x"), 256)
	for i := 0; i < cfg.PartitionNums; i++ {
		key := mustKeyForPartition(t, vlog, uint64(i))
		persistOneKV(t, vlog, i, &marshal.KV{
			Key: key,
			Data: &marshal.Data{
				Index:     uint64(i + 1),
				TimeStamp: time.Now().Unix(),
				Type:      marshal.TypeInsert,
				Value:     largeValue,
			},
		})
	}
	for _, p := range vlog.partitions {
		for _, sst := range p.SSTMap {
			if err = sst.Close(); err != nil {
				t.Fatalf("close sst failed: %v", err)
			}
		}
	}

	memCfg := config.MemConfig{
		MemTableSize: 64,
		Concurrency:  1,
	}
	active := newMemTable(memCfg)
	if putErr := active.Put(&marshal.BytesKV{
		Key: []byte("mem-key"),
		Value: marshal.EncodeData(&marshal.Data{
			Index:     10,
			TimeStamp: time.Now().Unix(),
			Type:      marshal.TypeInsert,
			Value:     []byte("mem-value"),
		}),
	}); putErr != nil {
		t.Fatalf("put mem key failed: %v", putErr)
	}

	db := &C2KV{
		activeMem: active,
		immtableQ: newMemTableQueue(4),
		valueLog:  vlog,
	}

	done := make(chan error, 1)
	go func() {
		_, scanErr := db.Scan([]byte("a"), []byte("z"))
		done <- scanErr
	}()

	select {
	case scanErr := <-done:
		if scanErr == nil {
			t.Fatalf("expected valueLog scan error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal(fmt.Errorf("db.Scan timed out while waiting for valueLog error"))
	}
}
