package mocks

import (
	"bytes"
	"fmt"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"path"
	"sort"
	"time"
)

const CreatKVsFmt = "create KVs nums %d, data valLen %d, bytes count %s"
const PartitionFormat = "PARTITION_%d"
const MinIndex = 1

var CurDirPath, _ = os.Getwd()
var DBPath = path.Join("./", "C2KV")
var PartitionDir1 = path.Join(CurDirPath, fmt.Sprintf(PartitionFormat, 1))
var PartitionDir2 = path.Join(CurDirPath, fmt.Sprintf(PartitionFormat, 2))
var PartitionDir3 = path.Join(CurDirPath, fmt.Sprintf(PartitionFormat, 3))

var VlogCfg = config.ValueLogConfig{ValueLogDir: ValueLogPath, PartitionNums: 3}

var KVs_67MB = CreateSortKVs(250000, 250, true)
var KVs_27KBNoDelOp = CreateSortKVs(100, 250, false)
var KVs_67MBNoDelOp = CreateSortKVs(250000, 250, true)
var KVs27KBNoDelOp = CreateSortKVs(100, 250, false)
var OneKV = CreateSortKVs(1, 250, false)[0]
var SingleKV_DELTE = CreateSingleKV(250, false)
var SingleKV = CreateSingleKV(250, false)
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

func CreateSingleKV(valLen int, Delete bool) *marshal.KV {
	key := genUniqueKey()
	kv := &marshal.KV{
		Key:     key,
		KeySize: uint32(len(key)),
		Data: &marshal.Data{
			Index:     uint64(1),
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
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
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
			ApplySig: 0,
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

func ConvertSize(size int) string {
	units := []string{"B", "KB", "MB", "GB"}
	if size == 0 {
		return "0" + units[0]
	}
	i := 0
	for size >= 1024 {
		size /= 1024
		i++
	}
	return fmt.Sprintf("%.f", float64(size)) + units[i]
}

func CreateKVs(num int, length int, hasDelete bool) []*marshal.KV {
	kvs := make([]*marshal.KV, 0)
	for i := 1; i < num; i++ {
		kv := &marshal.KV{
			ApplySig: 1,
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
		randomLetter := rand.Intn(26) + 97 // 小写字母
		data[i] = byte(randomLetter)
	}
	return data
}

func CreateRangeRandomIndex(min, max int) int {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	randomNumber := rand.Intn(max-min+1) + min
	return randomNumber
}
