package db

import (
	"fmt"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createRestoreTestDB(t *testing.T) *C2KV {
	t.Helper()
	dir := t.TempDir()
	segPath := filepath.Join(dir, fmt.Sprintf("%014d%s", 1, wal.SegSuffix))
	if err := os.WriteFile(segPath, make([]byte, 4096), 0644); err != nil {
		t.Fatalf("write segment file failed: %v", err)
	}

	w := wal.NewWal(config.WalConfig{
		WalDirPath:  dir,
		SegmentSize: 1,
	})
	if err := w.VlogStateSegment.Save(1); err != nil {
		t.Fatalf("save vlog state failed: %v", err)
	}
	if err := w.WalStateSegment.Save(1, 1); err != nil {
		t.Fatalf("save wal state failed: %v", err)
	}
	if err := w.RaftStateSegment.Save(pb.HardState{Term: 1, Vote: 1, Commit: 1}); err != nil {
		t.Fatalf("save raft state failed: %v", err)
	}

	memCfg := config.MemConfig{
		MemTableSize: 64,
		Concurrency:  1,
	}
	db := &C2KV{
		activeMem:    newMemTable(memCfg),
		immtableQ:    newMemTableQueue(4),
		memTablePipe: make(chan *memTable, 1),
		wal:          w,
	}
	db.memTablePipe <- newMemTable(memCfg)
	return db
}

func TestRestoreImMemTableSingleSegmentReturns(t *testing.T) {
	db := createRestoreTestDB(t)
	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("restoreImMemTable panicked: %v", r)
			}
		}()
		db.restoreImMemTable()
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restoreImMemTable timed out")
	}
}

func TestRestoreMemEntriesSingleSegmentReturns(t *testing.T) {
	db := createRestoreTestDB(t)
	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("restoreMemEntries panicked: %v", r)
			}
		}()
		db.restoreMemEntries()
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restoreMemEntries timed out")
	}
}
