package partition

import (
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSSTReadWrite(t *testing.T) {
	sst, err := OpenSST(createSSTFileName(filePath, NewSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	testData := []byte("test data")
	if err := sst.Write(testData); err != nil {
		t.Errorf("Failed to write to SST file: %v", err)
	}

	sst, err = OpenSST(sst.fName)
	readData, err := sst.Read(int64(len(testData)), 0)
	if err != nil {
		t.Errorf("Failed to read from SST file: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("Read data does not match written data")
	}
	sst.Close()
	sst.Remove()
}

func TestSSTRename(t *testing.T) {
	sst, err := OpenSST(createSSTFileName(filePath, TmpSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	sst.Rename(createSSTFileName(filePath, NewSST, None))
	if _, err := os.Stat(sst.fName); os.IsNotExist(err) {
		t.Errorf("Renamed SST file does not exist")
	}
}

func TestPartition_OpenOld(t *testing.T) {
	partitionDir := partitionDir1
	if _, err := os.Stat(partitionDir); err != nil {
		if err := os.Mkdir(partitionDir, 0755); err != nil {
			t.Fatalf("failed to create partition directory: %v", err)
		}
	}

	indexFilePath := filepath.Join(partitionDir, "test"+indexFileSuffixName)
	sstFile1Path := filepath.Join(partitionDir, "test1"+SSTFileSuffixName)
	sstFile2Path := filepath.Join(partitionDir, "test2"+SSTFileSuffixName)

	indexFile, _ := os.Create(indexFilePath)
	os.Create(sstFile1Path)
	os.Create(sstFile2Path)
	p := OpenPartition(partitionDir)
	if p.dirPath != partitionDir {
		t.Errorf("expected dirPath to be %s, got %s", partitionDir, p.dirPath)
	}

	if indexFile.Name() != p.indexer.Fp {
		t.Errorf("failed open  index file ")
	}
	os.RemoveAll(partitionDir)
}

func TestPartition_PersistKvs(t *testing.T) {
	CreatPartitionDirIfNotExist(partitionDir1)
	errC := make(chan error, 1)
	go func() {
		for {
			select {
			case err := <-errC:
				if err != nil {
					t.Error(err)
				}
			}
		}
	}()
	p := OpenPartition(partitionDir1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.PersistKvs(_67MBKVs, wg, errC)
	wg.Wait()
}

func TestPartition_Get(t *testing.T) {
	persisitKVs := _27KBKVsNoDelOp
	p := MockPartitionPersistKVs(partitionDir2, persisitKVs)

	index := createRandomIndex(1, 100)
	kv := _27KBKVsNoDelOp[index]
	kvRecive, err := p.Get(kv.Key)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, kv.Key, kvRecive.Key)
	assert.EqualValues(t, kv.Data.Value, kvRecive.Data.Value)
}

// todo fix
func TestPartition_Scan(t *testing.T) {
	persistKVs := _27KBKVsNoDelOp
	p := MockPartitionPersistKVs(partitionDir3, persistKVs)

	min := minIndex
	max := len(persistKVs) - 1
	kvs := make([]marshal.BytesKV, 0)
	lowIndex := createRandomIndex(min, max)
	lowKey := persistKVs[lowIndex].Key
	highKey := persistKVs[max].Key
	for lowIndex <= max {
		kv := _27KBKVsNoDelOp[lowIndex]
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
