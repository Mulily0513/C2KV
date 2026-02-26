package db

import (
	"sync"
	"testing"

	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/Mulily0513/C2KV/internal/db/wal"
)

func newApplyTestDB(t *testing.T, entries []*pb.Entry, appliedIndex uint64) *C2KV {
	t.Helper()
	memCfg := config.MemConfig{MemTableSize: 64, Concurrency: 1}
	w := wal.NewWal(config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
	})
	w.WalStateSegment.AppliedIndex = appliedIndex
	w.WalStateSegment.AppliedTerm = appliedIndex

	db := &C2KV{
		activeMem:    newMemTable(memCfg),
		immtableQ:    newMemTableQueue(4),
		memFlushC:    make(chan *memTable, 4),
		memTablePipe: make(chan *memTable, 1),
		wal:          w,
		entries:      append([]*pb.Entry(nil), entries...),
		mu:           sync.RWMutex{},
	}
	db.memTablePipe <- newMemTable(memCfg)

	t.Cleanup(func() {
		_ = db.wal.Close()
		_ = db.wal.Remove()
		_ = db.wal.RaftStateSegment.Close()
		_ = db.wal.WalStateSegment.Close()
		_ = db.wal.VlogStateSegment.Close()
	})
	return db
}

func TestC2KVApplyEmptyNoop(t *testing.T) {
	db := newApplyTestDB(t, nil, 0)
	if err := db.Apply(nil); err != nil {
		t.Fatalf("Apply(nil) returned error: %v", err)
	}
	if err := db.Apply([]*marshal.KV{}); err != nil {
		t.Fatalf("Apply(empty) returned error: %v", err)
	}
}

func TestC2KVApplySkipsOnFirstIndexMismatch(t *testing.T) {
	entries := mocks.CreateEntries(5, 32)
	db := newApplyTestDB(t, entries, 0)

	_, kvs := mocks.MockApplyData(2)
	kvs[0].Data.Index = 2 // expected 1

	if err := db.Apply(kvs); err != nil {
		t.Fatalf("Apply should skip non-contiguous batch without error, got %v", err)
	}
	if db.wal.WalStateSegment.AppliedIndex != 0 {
		t.Fatalf("AppliedIndex should remain unchanged on skipped batch, got %d", db.wal.WalStateSegment.AppliedIndex)
	}
}

func TestC2KVApplyTrimsWhenApplyRangeExceedsEntries(t *testing.T) {
	entries := mocks.CreateEntries(5, 32)
	db := newApplyTestDB(t, entries, 0)

	_, kvs := mocks.MockApplyData(6) // last index exceeds db entries
	if err := db.Apply(kvs); err != nil {
		t.Fatalf("Apply should trim suffix beyond in-memory entries, got %v", err)
	}
	if db.wal.WalStateSegment.AppliedIndex != 5 {
		t.Fatalf("AppliedIndex mismatch after trim, got=%d want=5", db.wal.WalStateSegment.AppliedIndex)
	}
	if db.wal.WalStateSegment.AppliedTerm != 5 {
		t.Fatalf("AppliedTerm mismatch after trim, got=%d want=5", db.wal.WalStateSegment.AppliedTerm)
	}
}

func TestC2KVApplyUsesPreEncodedDataBytes(t *testing.T) {
	entries := mocks.CreateEntries(5, 32)
	db := newApplyTestDB(t, entries, 0)

	_, kvs := mocks.MockApplyData(5)
	for _, kv := range kvs {
		kv.DataBytes = marshal.EncodeData(kv.Data)
		kv.Data = nil
	}

	if err := db.Apply(kvs); err != nil {
		t.Fatalf("Apply should accept pre-encoded DataBytes payload, got %v", err)
	}
	if db.wal.WalStateSegment.AppliedIndex != 5 {
		t.Fatalf("AppliedIndex mismatch after DataBytes apply, got=%d want=5", db.wal.WalStateSegment.AppliedIndex)
	}
	got, err := db.Get(kvs[4].Key)
	if err != nil {
		t.Fatalf("Get after DataBytes apply failed: %v", err)
	}
	if got == nil || got.Data == nil || got.Data.Index != 5 {
		t.Fatalf("unexpected decoded value after DataBytes apply, got=%+v", got)
	}
}

func TestC2KVTruncateWithNoEntries(t *testing.T) {
	db := newApplyTestDB(t, nil, 0)
	if err := db.Truncate(10); err != nil {
		t.Fatalf("Truncate should succeed when in-memory entries are empty, got %v", err)
	}
}
