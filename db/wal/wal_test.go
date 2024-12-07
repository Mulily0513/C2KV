package wal

import (
	"github.com/Mulily0513/C2KV/db/mocks"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWAL_Truncate(t *testing.T) {
	truncateIndex := uint64(25000)
	wal := NewWal(mocks.TestWALCfg1Size)
	ents := mocks.SplitEntries(10000, mocks.Entries61MB)
	for _, e := range ents {
		err := wal.Write(e)
		if err != nil {
			t.Fatal(err)
		}
	}

	truncBeforeSeg := wal.OrderSegmentList.find(truncateIndex)
	entries1 := readEntriesBySeg(truncBeforeSeg)

	wal.Truncate(truncateIndex)

	truncAfterSeg := wal.OrderSegmentList.find(truncateIndex)
	entries2 := readEntriesBySeg(truncAfterSeg)

	entries3 := make([]*pb.Entry, 0)
	for i := 0; i < len(entries1); i++ {
		if entries1[i].Index <= truncateIndex {
			entries3 = append(entries3, entries1[i])
		}
	}

	assert.EqualValues(t, entries2, entries3)
	wal.Close()
	wal.Remove()
}

func TestWAL_Write(t *testing.T) {
	var err error
	defer func() {
		if err != nil {
			t.Log(err)
		}
	}()
	wal := NewWal(mocks.TestWALCfg64Size)
	if err != nil {
		t.Fatal(err)
	}
	err = wal.Write(mocks.Entries61MB)
	err = wal.Close()
	err = wal.Remove()
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
