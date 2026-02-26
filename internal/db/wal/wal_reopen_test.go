package wal

import (
	"testing"

	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
)

func TestWALReopenRestoresEntriesAndStateSegments(t *testing.T) {
	cfg := config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
		SyncMode:    config.WALSyncAlways,
	}

	w1 := NewWal(cfg)
	ents := mocks.CreateEntries(12, 64)
	if err := w1.Write(ents); err != nil {
		t.Fatalf("write entries failed: %v", err)
	}

	wantHS := pb.HardState{Term: 7, Vote: 2, Commit: 11}
	if err := w1.RaftStateSegment.Save(wantHS); err != nil {
		t.Fatalf("save raft state failed: %v", err)
	}
	if err := w1.WalStateSegment.Save(10, 7); err != nil {
		t.Fatalf("save wal state failed: %v", err)
	}
	if err := w1.VlogStateSegment.Save(9); err != nil {
		t.Fatalf("save vlog state failed: %v", err)
	}

	if err := w1.Close(); err != nil {
		t.Fatalf("close wal failed: %v", err)
	}
	if err := w1.RaftStateSegment.Close(); err != nil {
		t.Fatalf("close raft state segment failed: %v", err)
	}
	if err := w1.WalStateSegment.Close(); err != nil {
		t.Fatalf("close wal state segment failed: %v", err)
	}
	if err := w1.VlogStateSegment.Close(); err != nil {
		t.Fatalf("close vlog state segment failed: %v", err)
	}

	w2 := NewWal(cfg)
	defer func() {
		_ = w2.Close()
		_ = w2.RaftStateSegment.Close()
		_ = w2.WalStateSegment.Close()
		_ = w2.VlogStateSegment.Close()
	}()

	if w2.RaftStateSegment.RaftState != wantHS {
		t.Fatalf("raft state mismatch after reopen, want=%+v got=%+v", wantHS, w2.RaftStateSegment.RaftState)
	}
	if w2.WalStateSegment.AppliedIndex != 10 || w2.WalStateSegment.AppliedTerm != 7 {
		t.Fatalf(
			"wal state mismatch after reopen, want(index=10,term=7) got(index=%d,term=%d)",
			w2.WalStateSegment.AppliedIndex,
			w2.WalStateSegment.AppliedTerm,
		)
	}
	if w2.VlogStateSegment.PersistIndex != 9 {
		t.Fatalf("vlog state mismatch after reopen, want=9 got=%d", w2.VlogStateSegment.PersistIndex)
	}

	seg := w2.OrderSegmentList.find(ents[0].Index)
	if seg == nil {
		t.Fatal("expected historical wal segment to be loaded on reopen")
	}
	got := readEntriesBySeg(seg)
	if len(got) != len(ents) {
		t.Fatalf("recovered entry count mismatch, want=%d got=%d", len(ents), len(got))
	}
	if got[0].Index != ents[0].Index || got[len(got)-1].Index != ents[len(ents)-1].Index {
		t.Fatalf(
			"recovered entry index range mismatch, want=[%d,%d] got=[%d,%d]",
			ents[0].Index,
			ents[len(ents)-1].Index,
			got[0].Index,
			got[len(got)-1].Index,
		)
	}
}
