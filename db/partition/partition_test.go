package partition

import (
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/db/mocks"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func MockPartitionWithPersistKVs(partitionDir string, persistKVs []*marshal.KV) *Partition {
	mocks.CreatPartitionDirIfNotExist(partitionDir)
	errC := make(chan error, 1)
	p := OpenPartition(mocks.PartitionDir3)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	p.PersistKvs(persistKVs, wg, errC)
	wg.Wait()
	if err := <-errC; err != nil {
		panic("failed persist kvs")
	}
	return p
}

func TestPartition_Get(t *testing.T) {
	persisitKVs := mocks.KVs_27KBNoDelOp
	p := MockPartitionWithPersistKVs(mocks.PartitionDir2, persisitKVs)

	index := mocks.CreateRandomIndex(100)
	kv := mocks.KVs_27KBNoDelOp[index]
	kvRecive, err := p.Get(kv.Key)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, kv.Key, kvRecive.Key)
	assert.EqualValues(t, kv.Data.Value, kvRecive.Data.Value)
}

func TestPartition_Scan(t *testing.T) {
	persistKVs := mocks.KVs27KBNoDelOp
	p := MockPartitionWithPersistKVs(mocks.PartitionDir2, persistKVs)

	min := 1
	max := len(persistKVs) - 1
	kvs := make([]marshal.BytesKV, 0)
	lowIndex := mocks.CreateRangeRandomIndex(min, max)
	lowKey := persistKVs[lowIndex].Key
	highKey := persistKVs[max].Key
	for lowIndex <= max {
		kv := persistKVs[lowIndex]
		kvs = append(kvs, marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value})
		lowIndex++
	}

	kvsScan, err := p.Scan(lowKey, highKey)
	if err != nil {
		return
	}

	kvsVerify := make([]marshal.BytesKV, 0)
	for _, kv := range kvsScan {
		kvsVerify = append(kvsVerify, marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value})
	}
	assert.EqualValues(t, kvs, kvsVerify)
}
