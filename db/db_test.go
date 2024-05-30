package db

import (
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/db/mocks"
	"github.com/Mulily0513/C2KV/db/wal"
	"reflect"
	"testing"
)

var MockDBCfg = &config.DBConfig{
	DBPath: mocks.DBPath,
	WalConfig: config.WalConfig{
		WalDirPath: mocks.WALPath,
	},
	ValueLogConfig: config.ValueLogConfig{
		ValueLogDir: mocks.ValueLogPath,
	},
	MemConfig: config.MemConfig{
		MemTableSize:     60,
		Concurrency:      3,
		MemTableNums:     10,
		MemTablePipeSize: 10,
	},
}

func MockKVStorage(dbCfg *config.DBConfig) (C2 *C2KV) {
	dbCfgCheck(dbCfg)
	C2 = new(C2KV)
	memFlushC := make(chan *MemTable, dbCfg.MemConfig.MemTableNums)
	C2.memTablePipe = make(chan *MemTable, dbCfg.MemConfig.MemTablePipeSize)
	C2.immtableQ = NewMemTableQueue(dbCfg.MemConfig.MemTableNums)
	C2.activeMem = NewMemTable(dbCfg.MemConfig)
	C2.memFlushC = memFlushC
	C2.wal = wal.NewWal(dbCfg.WalConfig)
	C2.valueLog = OpenValueLog(dbCfg.ValueLogConfig, memFlushC, C2.wal.KVStateSegment)
	go func() {
		for {
			C2.memTablePipe <- NewMemTable(dbCfg.MemConfig)
		}
	}()
	return
}

func TestKVStorage_dbCfgCheck(t *testing.T) {
	dbCfgCheck(MockDBCfg)
}

func TestKVStorage_PersistUnstableEnts(t *testing.T) {
	entSlices := mocks.ENTS_5GROUP_5000NUMS_250LENGTH
	C2KV := MockKVStorage(MockDBCfg)
	for _, ents := range entSlices {
		if err := C2KV.PersistUnstableEnts(ents); err != nil {
			t.Error(err)
		}
	}
}

func TestKVStorage_RestoreMemEntFromWAL(t *testing.T) {
	C2KV := MockKVStorage(MockDBCfg)
	PersisitIndex := 5665
	ApplyIndex := 11123
	CommittedIndex := 15666
	C2KV.wal.KVStateSegment.PersistIndex = uint64(PersisitIndex)
	C2KV.wal.KVStateSegment.AppliedIndex = uint64(ApplyIndex)
	C2KV.wal.RaftStateSegment.RaftState.Commit = uint64(CommittedIndex)
	C2KV.restoreMemEntries()
}

func TestKVStorage_RestoreImMemFromWAL(t *testing.T) {
	C2KV := MockKVStorage(MockDBCfg)
	PersisitIndex := 5665
	ApplyIndex := 11123
	CommittedIndex := 15666
	C2KV.wal.KVStateSegment.PersistIndex = uint64(PersisitIndex)
	C2KV.wal.KVStateSegment.AppliedIndex = uint64(ApplyIndex)
	C2KV.wal.RaftStateSegment.RaftState.Commit = uint64(CommittedIndex)
	C2KV.restoreImMemTable()
}

func TestKVStorage_KVOperate_GET(t *testing.T) {
	kvs := mocks.KVS_RAND_27KB_HASDEL_UQKey
	C2KV := MockKVStorage(MockDBCfg)
	err := C2KV.Apply(kvs)
	if err != nil {
		t.Error(err)
	}
	//获取验证集
	max := len(kvs) - 1
	Index := mocks.CreateRandomIndex(max)
	kv := kvs[Index]
	reKv, err := C2KV.Get(kv.Key)
	if err != nil {
		t.Error(err)
	}
	reflect.DeepEqual(kv.Data, reKv.Data)
}

func TestKVStorage_KVOperate_SCAN(t *testing.T) {
	kvs := mocks.KVS_RAND_27KB_HASDEL_UQKey
	C2KV := MockKVStorage(MockDBCfg)
	err := C2KV.Apply(kvs)
	if err != nil {
		t.Error(err)
	}

	//获取验证集
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

	allKvs, _ := C2KV.Scan(lowKey, highKey)
	reflect.DeepEqual(verifyKvs, allKvs)
}
