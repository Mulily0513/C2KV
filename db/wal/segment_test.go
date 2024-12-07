package wal

import (
	"fmt"
	"github.com/Mulily0513/C2KV/db/mocks"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestSegmentFile_newSegmentFile(t *testing.T) {
	segment := newSegmentFile(mocks.TestWALCfg1Size)
	assert.EqualValues(t, segment.Index, defaultMinLogIndex)
	assert.EqualValues(t, segment.blocksOffset, 0)
	assert.EqualValues(t, segment.WalDirPath, mocks.TestWALCfg1Size.WalDirPath)

	assert.NotEqual(t, segment.Fd, nil)
	assert.NotEqual(t, segment.blockPool, nil)
	_ = segment.close()
	_ = segment.remove()
}

func TestSegmentFile_BlockWrite(t *testing.T) {
	//todo
	// 1. Write <block4.
	// 2. Write >block4, <block8.
	// 3. Write greater than block8.
	// 4. Continuously write data of <block4.
	// 5. Continuously write data of >block4, <block8.
	// 6. Continuously write data greater than block8.
	tests := []struct {
		name string
		ents []*pb.Entry
	}{
		{"<block4",
			mocks.Entries_20_250_3KB,
		},
		{">block4, <block8",
			mocks.Entries5,
		},
		{">block8",
			mocks.Entries5,
		},
		{" series <block4",
			mocks.Entries5,
		},
		{" series >block4,<block8",
			mocks.Entries5,
		},
		{" series >block4,<block8",
			mocks.Entries5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segment := newSegmentFile(mocks.TestWALCfg1Size)
			data, bytesCount := mocks.MarshalWALEntries(tt.ents)
			if err := segment.write(data, bytesCount, tt.ents[0].Index); err != nil {
				t.Error(err)
			}
			assert.EqualValues(t, len(segment.blocks), bytesCount+segment.BlocksRemainSize)
			assert.EqualValues(t, bytesCount, segment.blocksOffset)
		})
	}
}

func TestSegmentFile_SerialWrite(t *testing.T) {
	tests := []struct {
		name string
		ents []*pb.Entry
		nums int
	}{
		{"<block4",
			mocks.Entries1,
			3,
		},
	}
	for _, tt := range tests {
		var totalCounts int
		t.Run(tt.name, func(t *testing.T) {
			segment := newSegmentFile(mocks.TestWALCfg1Size)
			data, bytesCount := mocks.MarshalWALEntries(tt.ents)
			for i := 0; i < tt.nums; i++ {
				if err := segment.write(data, bytesCount, tt.ents[0].Index); err != nil {
					t.Error(err)
				}
				totalCounts += bytesCount
			}
			assert.EqualValues(t, len(segment.blocks), totalCounts+segment.BlocksRemainSize)
			assert.EqualValues(t, totalCounts, segment.blocksOffset)
		})
	}
}

func TestSegmentReader_Block4(t *testing.T) {
	segment := newSegmentFile(mocks.TestWALCfg1Size)
	MockSegmentWrite(mocks.Entries_20_250_3KB, segment)
	reader := NewSegmentReader(segment)
	ents := make([]*pb.Entry, 0)
	assert.EqualValues(t, segment.blocks, reader.blocks)
	for {
		header, err := reader.ReadHeader()
		if err != nil && err.Error() == "EOF" {
			break
		}
		entry, err := reader.ReadEntry(header)
		if err != nil {
			t.Error(err)
		}
		reader.Next(header.EntrySize)
		ents = append(ents, entry)
	}
	assert.EqualValues(t, mocks.Entries_20_250_3KB, ents)
}

func TestSegmentReader_Blocks(t *testing.T) {
	segment := newSegmentFile(mocks.TestWALCfg1Size)
	MockSegmentWrite(mocks.Entries61MB, segment)
	reader := NewSegmentReader(segment)
	ents := make([]*pb.Entry, 0)
	for {
		header, err := reader.ReadHeader()
		if err != nil && err.Error() == "EOF" {
			break
		}
		entry, err := reader.ReadEntry(header)
		if err != nil {
			t.Error(err)
		}
		reader.Next(header.EntrySize)
		ents = append(ents, entry)
	}
	assert.EqualValues(t, mocks.Entries61MB, ents)
}

func TestOrderedSegmentList(t *testing.T) {
	// Create a new OrderedSegmentList
	oll := newOrderedSegmentList()

	// Create some segments
	seg1 := &segment{Index: 1}
	seg2 := &segment{Index: 2}
	seg3 := &segment{Index: 3}
	seg4 := &segment{Index: 5}
	seg5 := &segment{Index: 9}
	seg6 := &segment{Index: 6}

	// Insert segments into the OrderedSegmentList
	oll.insert(seg2)
	oll.insert(seg1)
	oll.insert(seg3)
	oll.insert(seg5)
	oll.insert(seg6)
	oll.insert(seg4)

	// Test Find method
	foundSeg := oll.find(2)
	if foundSeg != seg2 {
		t.Errorf("Expected segment with index 2, but got segment with index %d", foundSeg.Index)
	}

	// Test Find method with non-existent index
	FoundSeg := oll.find(4)
	if FoundSeg.Index != 3 {
		t.Errorf("Expected segment with index 3, but got segment with index %d", FoundSeg.Index)
	}

	for oll.Head != nil {
		fmt.Println(oll.Head.Seg.Index)
		oll.Head = oll.Head.Next
	}
}

func TestVlogStateSegment(t *testing.T) {
	//test encode
	vlogSeg, err := OpenVlogStateSegment(filepath.Join(mocks.TestWALCfg1Size.WalDirPath, uuid.New().String()+VlogSuffix))
	if err != nil {
		panic(err)
	}
	vlogSeg.Save(1)
	assert.EqualValues(t, 1, vlogSeg.PersistIndex)

	//test  decode
	fpath := vlogSeg.fd.Name()
	vlogSeg.Close()
	vlogSeg, err = OpenVlogStateSegment(fpath)
	if err != nil {
		panic(err)
	}
	assert.EqualValues(t, 1, vlogSeg.PersistIndex)
	vlogSeg.Remove()
}

func TestWALStateSegment(t *testing.T) {
	//test encode
	walSeg, err := OpenWalStateSegment(filepath.Join(mocks.TestWALCfg1Size.WalDirPath, uuid.New().String()+WALSuffix))
	if err != nil {
		panic(err)
	}
	walSeg.Save(1, 2)
	assert.EqualValues(t, 1, walSeg.AppliedIndex)
	assert.EqualValues(t, 2, walSeg.AppliedTerm)

	//test  decode
	fpath := walSeg.fd.Name()
	walSeg.Close()
	walSeg, err = OpenWalStateSegment(fpath)
	if err != nil {
		panic(err)
	}
	assert.EqualValues(t, 1, walSeg.AppliedIndex)
	assert.EqualValues(t, 2, walSeg.AppliedTerm)
	walSeg.Remove()
}

func TestRaftStateSegment(t *testing.T) {
	//test encode
	raftSeg, err := openRaftStateSegment(filepath.Join(mocks.TestWALCfg1Size.WalDirPath, uuid.New().String()+RaftSuffix))
	if err != nil {
		panic(err)
	}
	hs := pb.HardState{Term: 1, Vote: 2, Commit: 3}
	raftSeg.Save(hs)
	assert.EqualValues(t, hs, raftSeg.RaftState)

	//test decode
	fpath := raftSeg.fd.Name()
	raftSeg.Close()
	raftSeg, err = openRaftStateSegment(fpath)
	if err != nil {
		panic(err)
	}
	assert.EqualValues(t, hs, raftSeg.RaftState)
	raftSeg.Remove()
}
