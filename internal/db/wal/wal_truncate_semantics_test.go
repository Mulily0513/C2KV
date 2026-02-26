package wal

import (
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"testing"
)

func TestWALTruncateRemovesTruncateIndexEntry(t *testing.T) {
	cfg := config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
	}
	w := NewWal(cfg)
	defer func() {
		_ = w.Close()
		_ = w.Remove()
	}()

	ents := mocks.CreateEntries(20, 64)
	if err := w.Write(ents); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := w.Truncate(10); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	seg := w.OrderSegmentList.find(9)
	if seg == nil {
		t.Fatal("expected segment to exist after truncate")
	}
	got := readEntriesBySeg(seg)
	if len(got) != 9 {
		t.Fatalf("expected 9 entries after truncate@10, got %d", len(got))
	}
	if got[len(got)-1].Index != 9 {
		t.Fatalf("expected last index=9 after truncate@10, got %d", got[len(got)-1].Index)
	}
}
