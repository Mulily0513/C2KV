package partition

import (
	"bytes"
	"fmt"
	"github.com/Mulily0513/C2KV/db/marshal"
	"math/rand"
	"os"
	"path"
	"sort"
	"sync"
	"testing"
	"time"
)

const CreatKVsFmt = "create KVs nums %d, data length %d, bytes count %s"
const PartitionFormat = "PARTITION_%d"
const minIndex = 0

var filePath, _ = os.Getwd()
var partitionDir1 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 1))
var partitionDir2 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 2))
var partitionDir3 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 3))
var _67MBKVs = CreateKVs(250000, 250, true)
var _27KBKVsNoDelOp = CreateKVs(100, 250, false)
var _67MBKVsNoDelOp = CreateKVs(250000, 250, true)

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

func SortKVSByKey(kvs []*marshal.KV) []*marshal.KV {
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
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

func createRandomIndex(min, max int) int {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	randomNumber := rand.Intn(max-min+1) + min
	return randomNumber
}

func MockPartitionPersistKVs(partitionDir string, persistKVs []*marshal.KV) *Partition {
	CreatPartitionDirIfNotExist(partitionDir)
	errC := make(chan error, 1)
	p := OpenPartition(partitionDir3)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.PersistKvs(persistKVs, wg, errC)
	wg.Wait()
	return p
}

func TestCreateKvs(t *testing.T) {
	nums := 100
	length := 250
	kvs := CreateKVs(nums, length, true)
	fmt.Printf(CreatKVsFmt, nums, length, ConvertSize(marshalKVs(kvs)))
}
