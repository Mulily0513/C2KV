package mocks

import (
	"bytes"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"sort"
	"time"
)

const CreatKVsFmt = "create KVs nums %d, data valLen %d, bytes count %s"
const MinIndex = 1

var CurDirPath, _ = os.Getwd()

// kvs
var KVs_67MB = CreateSortKVs(250000, 250, true)
var KVs_27KBNoDelOp = CreateSortKVs(100, 250, false)
var KVs_67MBNoDelOp = CreateSortKVs(250000, 250, true)
var KVs27KBNoDelOp = CreateSortKVs(100, 250, false)
var OneKV = CreateSortKVs(1, 250, false)[0]
var SingleKV_DELTE = CreateSingleKV(250, false, 1)
var SingleKV = CreateSingleKV(250, false, 1)
var KVS_RAND_27KB_HASDEL_UQKey = CreateRandKVs(100, 250, true)
var KVS_SORT_27KB_NODEL_UQKey = CreateSortKVs(100, 250, false)
var KVS_SORT_27KB_HASDEL_UQKey = CreateSortKVs(100, 250, false)
var KVS_RAND_35MB_HASDEL_UQKey = CreateRandKVs(125000, 250, true)
var KVS_RAND_35MB_NODEL_UQKey = CreateRandKVs(125000, 250, false)

func genUniqueKey() []byte {
	return []byte(uuid.New().String())
}

func genRanValByte(valLen int) []byte {
	rand.Seed(time.Now().UnixNano())
	data := make([]byte, valLen)
	for i := 0; i < valLen; i++ {
		randomLetter := rand.Intn(26) + 97 // 小写字母
		data[i] = byte(randomLetter)
	}
	return data
}

func CreateSingleKV(valLen int, Delete bool, index uint64) *marshal.KV {
	key := genUniqueKey()
	kv := &marshal.KV{
		Key:     key,
		KeySize: uint32(len(key)),
		Data: &marshal.Data{
			Index:     index,
			TimeStamp: time.Now().Unix(),
			Type:      marshal.TypeInsert,
			Value:     genRanValByte(valLen),
		},
	}
	if Delete {
		kv.Data.Type = marshal.TypeDelete
	}
	return kv
}

func CreateRandKVs(num int, valLen int, hasDelete bool) []*marshal.KV {
	kvs := make([]*marshal.KV, 0)
	for i := 1; i <= num; i++ {
		key := genUniqueKey()
		kv := &marshal.KV{
			Key:     genUniqueKey(),
			KeySize: uint32(len(key)),
			Data: &marshal.Data{
				Index:     uint64(i),
				TimeStamp: time.Now().Unix(),
				Type:      marshal.TypeInsert,
				Value:     genRanValByte(valLen),
			},
		}
		if hasDelete && i%5 == 0 {
			kv.Data.Type = marshal.TypeDelete
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func CreateSortKVs(num int, valLen int, hasDelete bool) []*marshal.KV {
	return SortKVSByKey(CreateRandKVs(num, valLen, hasDelete))
}

func SortKVSByKey(kvs []*marshal.KV) []*marshal.KV {
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
}

func CreateRandomIndex(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}

func KVsTransToByteKVs(kvs []*marshal.KV) []*marshal.BytesKV {
	bytesKvs := make([]*marshal.BytesKV, 0)
	for _, kv := range kvs {
		bytesKvs = append(bytesKvs, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeData(kv.Data)})
	}
	return bytesKvs
}

func CreateApplyKVs(num int, valLen int, hasDelete bool) (kvs []*marshal.KV, ents []*pb.Entry) {
	kvs = make([]*marshal.KV, 0)
	ents = make([]*pb.Entry, 0)
	for i := 1; i <= num; i++ {
		key := genUniqueKey()
		kv := &marshal.KV{
			ApplySig: []byte(uuid.New().String()),
			Key:      key,
			KeySize:  uint32(len(key)),
			Data: &marshal.Data{
				Index:     uint64(i),
				TimeStamp: time.Now().Unix(),
				Type:      0,
				Value:     genRanValByte(valLen),
			},
		}
		if hasDelete && i%5 == 0 {
			kv.Data.Type = marshal.TypeDelete
		}
		kvs = append(kvs, kv)

		ents = append(ents, &pb.Entry{Index: uint64(i), Data: marshal.EncodeKV(kv)})
	}
	return kvs, ents
}

func CreateEnts(num int, firstIndex int) (ents []*pb.Entry) {
	ents = make([]*pb.Entry, 0)
	for i := firstIndex; i < num+firstIndex; i++ {
		ents = append(ents, &pb.Entry{Index: uint64(i), Data: genUniqueKey()})
	}
	return ents
}

func CreatPartitionDirIfNotExist(partitionDir string) {
	if _, err := os.Stat(partitionDir); err != nil {
		if err := os.Mkdir(partitionDir, 0755); err != nil {
			println(err)
		}
	}
}

func marshalKVs(kvs []*marshal.KV) (byteCount int) {
	for _, kv := range kvs {
		byteCount += len(marshal.EncodeKV(kv))
	}
	return
}

func CreateKVs(num int, length int, hasDelete bool) []*marshal.KV {
	kvs := make([]*marshal.KV, 0)
	for i := 1; i < num; i++ {
		kv := &marshal.KV{
			ApplySig: []byte(uuid.New().String()),
			Key:      generateByte(5),
			Data: &marshal.Data{
				Index:     uint64(i),
				TimeStamp: time.Now().Unix(),
				Type:      0,
				Value:     generateByte(length),
			},
		}
		if hasDelete && i%2 == 0 {
			kv.Data.Type = marshal.TypeDelete
		}
		kvs = append(kvs, kv)
	}
	return SortKVSByKey(kvs)
}

func generateByte(length int) []byte {
	rand.Seed(time.Now().UnixNano())
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		randomLetter := rand.Intn(26) + 97
		data[i] = byte(randomLetter)
	}
	return data
}

func CreateRangeRandomIndex(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(max-min+1) + min
	return randomNumber
}

/*
/mock entry
*/

const CreatEntriesFmt = "create entries nums %d, data length %d, bytes count %s"

var Entries61MB = CreateEntries(50000, 250)
var Entries133MB = CreateEntries(500000, 250)
var Entries1MB = CreateEntries(5000, 250)
var Entries5 = CreateEntries(5, 250)
var Entries1 = CreateEntries(1, 250)
var Entries_20_250_3KB = CreateEntries(20, 250)
var ENTS_5GROUP_5000NUMS = CreateEntriesSlice(5, 5000, 250)

func CreateEntries(num int, valLen int) []*pb.Entry {
	entries := make([]*pb.Entry, num)
	for i := 1; i <= num; i++ {
		entry := &pb.Entry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  pb.EntryNormal,
			Data:  genRandomKVBytes(valLen, uint64(i)),
		}
		entries[i-1] = entry
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
			Data:  genRandomKVBytes(length, 1),
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

func genRandomKVBytes(length int, index uint64) []byte {
	return marshal.EncodeKV(CreateSingleKV(length, false, index))
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

func MockApplyData(num int) ([]*pb.Entry, []*marshal.KV) {
	kvs := make([]*marshal.KV, 0)
	ents := make([]*pb.Entry, 0)
	for i := 1; i <= num; i++ {
		kv := CreateSingleKV(250, false, uint64(i))
		ent := &pb.Entry{uint64(i), uint64(i), pb.EntryNormal, marshal.EncodeKV(kv)}
		kvs = append(kvs, kv)
		ents = append(ents, ent)
	}
	return ents, kvs
}
