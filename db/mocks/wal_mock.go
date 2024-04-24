package mocks

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
)

const CreatEntriesFmt = "create entries nums %d, data length %d, bytes count %s"

var Entries61MB = CreateEntries(50000, 250)
var Entries133MB = CreateEntries(500000, 250)
var Entries1MB = CreateEntries(5000, 250)
var Entries5 = CreateEntries(5, 250)
var Entries20 = CreateEntries(40, 250)
var ENTS_5GROUP_5000NUMS_250LENGTH = CreateEntriesSlice(5, 5000, 250)

func CreateEntries(num int, length int) []*pb.Entry {
	entries := make([]*pb.Entry, num)
	for i := 0; i < num; i++ {
		entry := &pb.Entry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  pb.EntryNormal,
			Data:  genKVBytes(length),
		}
		entries[i] = entry
	}
	return entries
}

func CreateEntriesSlice(groupNum, num int, length int) [][]*pb.Entry {
	groupEntries := make([][]*pb.Entry, groupNum)
	entries := make([]*pb.Entry, num*groupNum)
	for i := 0; i < num*groupNum; i++ {
		entry := &pb.Entry{
			Term:  uint64(i + 1),
			Index: uint64(i + 1),
			Type:  pb.EntryNormal,
			Data:  genKVBytes(length),
		}
		entries[i] = entry
	}

	for i := 0; i < groupNum; i++ {
		groupEntries[i] = entries[i*num : (i+1)*num]
	}
	return groupEntries
}

func SplitEntries(interval int, entries []*pb.Entry) [][]*pb.Entry {
	totalEntries := make([][]*pb.Entry, 0)
	count := 0
	subEntries := make([]*pb.Entry, 0)
	for _, e := range entries {
		if count <= interval {
			subEntries = append(subEntries, e)
			count++
		} else {
			totalEntries = append(totalEntries, subEntries)
			subEntries = make([]*pb.Entry, 0)
			count = 0
		}
	}
	return totalEntries
}

func genKVBytes(length int) []byte {
	return marshal.EncodeKV(CreateSingleKV(length, false))
}

func MarshalWALEntries(entries1 []*pb.Entry) (data []byte, bytesCount int) {
	data = make([]byte, 0)
	for _, e := range entries1 {
		wEntBytes, n := marshal.EncodeWALEntry(e)
		data = append(data, wEntBytes...)
		bytesCount += n
	}
	return
}
