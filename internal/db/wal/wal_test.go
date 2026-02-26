package wal

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWAL_Truncate(t *testing.T) {
	cfg := config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
	}
	truncateIndex := uint64(50)
	wal := NewWal(cfg)
	ents := mocks.SplitEntries(20, mocks.CreateEntries(100, 64))
	for _, e := range ents {
		err := wal.Write(e)
		if err != nil {
			t.Fatal(err)
		}
	}

	truncBeforeSeg := wal.activeSegment
	entries1 := readEntriesBySeg(truncBeforeSeg)

	wal.Truncate(truncateIndex)

	truncAfterSeg := wal.OrderSegmentList.find(truncateIndex)
	entries2 := readEntriesBySeg(truncAfterSeg)

	entries3 := make([]*pb.Entry, 0)
	for i := 0; i < len(entries1); i++ {
		if entries1[i].Index < truncateIndex {
			entries3 = append(entries3, entries1[i])
		}
	}

	assert.EqualValues(t, entries2, entries3)
	_ = wal.Close()
	_ = wal.Remove()
}

func TestWAL_Write(t *testing.T) {
	cfg := config.WalConfig{
		WalDirPath:  t.TempDir(),
		SegmentSize: 1,
	}
	wal := NewWal(cfg)
	if err := wal.Write(mocks.CreateEntries(1000, 64)); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Remove(); err != nil {
		t.Fatal(err)
	}
}

func readEntriesBySeg(segment *segment) (entries []*pb.Entry) {
	reader := NewSegmentReader(segment)
	for {
		header, err := reader.ReadHeader()
		if err != nil && err.Error() == "EOF" {
			break
		}
		entry, err := reader.ReadEntry(header)
		if err != nil {
			println(err)
		}
		reader.Next(header.EntrySize)
		entries = append(entries, entry)
	}
	return
}

func MockSegmentWrite(entries []*pb.Entry, segment *segment) {
	data, bytesCount := mocks.MarshalWALEntries(entries)
	segment.write(data, bytesCount, entries[0].Index)
}
