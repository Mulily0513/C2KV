package db

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"testing"
)

func TestC2KVTruncateKeepsEntriesBeforeIndex(t *testing.T) {
	w := wal.NewWal(config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
	})
	defer func() {
		_ = w.Close()
		_ = w.Remove()
	}()

	ents := mocks.CreateEntries(10, 32)
	if err := w.Write(ents); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	db := &C2KV{
		wal:     w,
		entries: append([]*pb.Entry(nil), ents...),
	}
	if err := db.Truncate(6); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}
	if len(db.entries) != 5 {
		t.Fatalf("expected 5 in-memory entries after truncate@6, got %d", len(db.entries))
	}
	if db.entries[len(db.entries)-1].Index != 5 {
		t.Fatalf("expected last in-memory index=5, got %d", db.entries[len(db.entries)-1].Index)
	}
}
