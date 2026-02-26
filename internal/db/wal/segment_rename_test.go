package wal

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"os"
	"testing"
)

func TestSegmentWriteRenamesByFirstIndex(t *testing.T) {
	cfg := config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
	}
	seg := newSegmentFile(cfg)
	t.Cleanup(func() {
		_ = seg.close()
	})

	ent := &pb.Entry{
		Term:  1,
		Index: 123,
		Type:  pb.EntryNormal,
		Data:  []byte("x"),
	}
	data, n := marshal.EncodeWALEntry(ent)
	if err := seg.write(data, n, ent.Index); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if seg.Index != ent.Index {
		t.Fatalf("expected segment index=%d, got %d", ent.Index, seg.Index)
	}
	if _, err := os.Stat(segmentFileName(cfg.WalDirPath, ent.Index)); err != nil {
		t.Fatalf("expected segment file by first index to exist: %v", err)
	}
}
