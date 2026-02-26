package db

import (
	"fmt"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"io"
	"testing"
	"time"
)

func restoreTestEntry(index uint64) *pb.Entry {
	kv := &marshal.KV{
		ApplySig: []byte("00000000-0000-0000-0000-000000000000"),
		Key:      []byte(fmt.Sprintf("k%d", index)),
		Data: &marshal.Data{
			Index:     index,
			TimeStamp: time.Now().Unix(),
			Type:      marshal.TypeInsert,
			Value:     []byte(fmt.Sprintf("v%d", index)),
		},
	}
	return &pb.Entry{
		Term:  1,
		Index: index,
		Type:  pb.EntryNormal,
		Data:  marshal.EncodeKV(kv),
	}
}

func createRestoreSemanticsDB(t *testing.T, persist, applied, committed uint64) *C2KV {
	t.Helper()
	dir := t.TempDir()
	w := wal.NewWal(config.WalConfig{
		WalDirPath:  dir,
		SegmentSize: 1,
	})

	ents := make([]*pb.Entry, 0, 6)
	for i := uint64(1); i <= 6; i++ {
		ents = append(ents, restoreTestEntry(i))
	}
	if err := w.Write(ents); err != nil {
		t.Fatalf("wal write failed: %v", err)
	}
	if err := w.VlogStateSegment.Save(persist); err != nil {
		t.Fatalf("save persist index failed: %v", err)
	}
	if err := w.WalStateSegment.Save(applied, 1); err != nil {
		t.Fatalf("save applied index failed: %v", err)
	}
	if err := w.RaftStateSegment.Save(pb.HardState{Term: 1, Vote: 1, Commit: committed}); err != nil {
		t.Fatalf("save raft state failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal failed: %v", err)
	}
	// Reopen to simulate restart: historical segments are loaded into OrderSegmentList.
	w = wal.NewWal(config.WalConfig{
		WalDirPath:  dir,
		SegmentSize: 1,
	})
	if w.OrderSegmentList.Head == nil {
		t.Fatal("expected at least one wal segment loaded on reopen")
	}
	reader := wal.NewSegmentReader(w.OrderSegmentList.Head.Seg)
	readCount := 0
	for {
		header, err := reader.ReadHeader()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read wal header failed: %v", err)
		}
		readCount++
		reader.Next(header.EntrySize)
	}
	if readCount == 0 {
		t.Fatal("expected wal segment contains entries, got 0")
	}

	memCfg := config.MemConfig{
		MemTableSize: 64,
		Concurrency:  1,
	}
	activeMem := newMemTable(memCfg)
	activeMem.cfg.MemTableSize = 1 << 20
	db := &C2KV{
		activeMem:    activeMem,
		immtableQ:    newMemTableQueue(4),
		memTablePipe: make(chan *memTable, 1),
		wal:          w,
	}
	return db
}

func TestRestoreImMemTableRangeSemantics(t *testing.T) {
	// Expect [persist, applied] = [2,4] to be restored.
	db := createRestoreSemanticsDB(t, 2, 4, 5)
	if db.wal.VlogStateSegment.PersistIndex != 2 {
		t.Fatalf("unexpected persist index: %d", db.wal.VlogStateSegment.PersistIndex)
	}
	if db.wal.WalStateSegment.AppliedIndex != 4 {
		t.Fatalf("unexpected applied index: %d", db.wal.WalStateSegment.AppliedIndex)
	}
	if db.wal.RaftStateSegment.RaftState.Commit != 5 {
		t.Fatalf("unexpected commit index: %d", db.wal.RaftStateSegment.RaftState.Commit)
	}
	db.restoreImMemTable()

	for _, idx := range []uint64{2, 3, 4} {
		key := []byte(fmt.Sprintf("k%d", idx))
		kv, ok := db.activeMem.Get(key)
		if !ok {
			t.Fatalf("expected key %q restored in immemtable", string(key))
		}
		if string(kv.Data.Value) != fmt.Sprintf("v%d", idx) {
			t.Fatalf("unexpected value for key %q: %q", string(key), string(kv.Data.Value))
		}
	}
	for _, idx := range []uint64{1, 5, 6} {
		key := []byte(fmt.Sprintf("k%d", idx))
		if _, ok := db.activeMem.Get(key); ok {
			t.Fatalf("did not expect key %q in restored immemtable", string(key))
		}
	}
}

func TestRestoreMemEntriesRangeSemantics(t *testing.T) {
	// Expect [applied, committed] = [4,5] to be restored into db.entries.
	db := createRestoreSemanticsDB(t, 2, 4, 5)
	if db.wal.WalStateSegment.AppliedIndex != 4 {
		t.Fatalf("unexpected applied index: %d", db.wal.WalStateSegment.AppliedIndex)
	}
	if db.wal.RaftStateSegment.RaftState.Commit != 5 {
		t.Fatalf("unexpected commit index: %d", db.wal.RaftStateSegment.RaftState.Commit)
	}
	db.restoreMemEntries()

	if len(db.entries) != 2 {
		t.Fatalf("expected 2 restored entries, got %d", len(db.entries))
	}
	if db.entries[0].Index != 4 || db.entries[1].Index != 5 {
		t.Fatalf("unexpected restored entry indexes: [%d, %d]", db.entries[0].Index, db.entries[1].Index)
	}
}
