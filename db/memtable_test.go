package db

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/mocks"
	"reflect"
	"testing"

	"github.com/ColdToo/Cold2DB/config"
)

var TestMemConfig = config.MemConfig{
	MemTableSize: 64,
	Concurrency:  8,
}

func TestMemTable_Scan(t *testing.T) {
	//获取验证集
	kvs := mocks.KVS_RAND_35MB_HASDEL_UQKey
	max := len(kvs) - 1
	verifyKvs := make([]*marshal.KV, 0)
	lowIndex := mocks.CreateRandomIndex(max)
	lowKey := kvs[lowIndex].Key
	highKey := kvs[max].Key
	for lowIndex <= max {
		kv := kvs[lowIndex]
		verifyKvs = append(verifyKvs, kv)
		lowIndex++
	}

	mem := NewMemTable(TestMemConfig)
	//获取测试集
	sklIter := mem.newSklIter()
	for _, kv := range kvs {
		sklIter.Put(kv.Key, marshal.EncodeData(kv.Data))
	}
	allKvs, _ := mem.Scan(lowKey, highKey)
	reflect.DeepEqual(kvs, allKvs)
}

func TestMemTable_All(t *testing.T) {
	kvs := mocks.KVS_RAND_35MB_HASDEL_UQKey
	bytesKvs := make([]*marshal.BytesKV, 0)
	for _, kv := range kvs {
		bytesKvs = append(bytesKvs, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeData(kv.Data)})
	}

	mem := NewMemTable(TestMemConfig)
	mem.ConcurrentPut(bytesKvs)

	allKvs := mem.All()
	verifyKvs := make([]marshal.KV, 0)
	for _, kv := range allKvs {
		verifyKvs = append(verifyKvs, marshal.KV{Key: kv.Key, KeySize: len(kv.Key), Data: marshal.DecodeData(kv.Value)})
	}

	reflect.DeepEqual(kvs, allKvs)
}

func TestMemTable_Get(t *testing.T) {
	kv := mocks.OneKV
	mem := NewMemTable(TestMemConfig)
	sklIter := mem.newSklIter()
	err := sklIter.Put(kv.Key, marshal.EncodeData(kv.Data))
	if err != nil {
		t.Error(err)
	}
	kv1, flag := mem.Get(kv.Key)
	if !flag {
		t.Error("should found")
	}
	reflect.DeepEqual(kv.Data, kv1.Data)
	data1 := kv1.Data
	data1.Index = 111
	data1.Type = 1
	data1.Value = []byte("111111111111111111111111111111111111111111111111111")
	err = sklIter.Put(kv.Key, marshal.EncodeData(data1))
	if err != nil {
		t.Error(err)
	}
	kv2, flag := mem.Get(kv.Key)
	if !flag {
		t.Error("should found")
	}
	reflect.DeepEqual(data1, kv2.Data)
}

func TestMemTable_Queue(t *testing.T) {
	queue := NewMemTableQueue(3)
	table1 := &MemTable{}
	table2 := &MemTable{}
	table3 := &MemTable{}

	queue.Enqueue(table1)
	queue.Enqueue(table2)
	queue.Enqueue(table3)

	if queue.size != 3 {
		t.Errorf("Expected queue size to be 3, but got %d", queue.size)
	}

	dequeuedTable := queue.Dequeue()
	if dequeuedTable != table1 {
		t.Error("Dequeued table does not match expected table")
	}

	if queue.size != 2 {
		t.Errorf("Expected queue size to be 2 after dequeue, but got %d", queue.size)
	}
}
