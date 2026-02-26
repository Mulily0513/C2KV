package db

import (
	"bytes"
	"fmt"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func mustKeyForPartition(t *testing.T, vlog *ValueLog, target uint64) []byte {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("p-%d-%d", target, i))
		if vlog.getKeyPartition(key) == target {
			return key
		}
	}
	t.Fatalf("cannot find key for partition %d", target)
	return nil
}

func persistOneKV(t *testing.T, vlog *ValueLog, pIdx int, kv *marshal.KV) {
	t.Helper()
	errC := make(chan error, 1)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go vlog.partitions[pIdx].PersistKvs([]*marshal.KV{kv}, wg, errC)
	wg.Wait()
	select {
	case err := <-errC:
		t.Fatalf("persist kv failed: %v", err)
	default:
	}
}

func TestValueLogScanMultiPartitionErrorDoesNotHang(t *testing.T) {
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

	// Ensure both partitions have large values (force SST read path on Scan).
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

	// Close all SST files so each partition Scan path returns read error.
	for _, p := range vlog.partitions {
		for _, sst := range p.SSTMap {
			if err = sst.Close(); err != nil {
				t.Fatalf("close sst failed: %v", err)
			}
		}
	}

	done := make(chan error, 1)
	go func() {
		_, scanErr := vlog.Scan([]byte("p-"), []byte("q-"))
		done <- scanErr
	}()

	select {
	case scanErr := <-done:
		if scanErr == nil {
			t.Fatal("expected scan error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("vlog scan hung when multiple partitions returned errors")
	}
}
