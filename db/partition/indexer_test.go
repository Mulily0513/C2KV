package partition

import (
	"github.com/Mulily0513/C2KV/code"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestBtreeIndexer_Get(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(filepath.Join(getwd, uuid.New().String()+indexFileSuffixName))
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Remove()

	tx, err := indexer.BeginTx()

	key := []byte("testKey")
	value := []byte("testValue")
	ops := make([]Op, 0) //(Insert, &arshal.BytesKV{Key: key, Value: value})
	ops = append(ops, Op{Insert, marshal.BytesKV{Key: key, Value: value}})

	err = indexer.ExecuteOps(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	meta, err := indexer.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(meta, marshal.BytesKV{Key: key, Value: value}) {
		t.Errorf("Get() returned unexpected value, expected: %v, got: %v", marshal.BytesKV{Key: key, Value: value}, meta)
	}
}

func TestBtreeIndexer_Scan(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(filepath.Join(getwd, uuid.New().String()+indexFileSuffixName))
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Remove()

	tx, err := indexer.BeginTx()
	low := []byte("a")
	high := []byte("z")
	ops := make([]Op, 0)
	ops = append(ops, Op{Insert, marshal.BytesKV{Key: []byte("b"), Value: []byte("value1")}})
	ops = append(ops, Op{Insert, marshal.BytesKV{Key: []byte("c"), Value: []byte("value2")}})
	ops = append(ops, Op{Insert, marshal.BytesKV{Key: []byte("d"), Value: []byte("value3")}})
	err = indexer.ExecuteOps(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	metaList, err := indexer.Scan(low, high)
	if err != nil {
		t.Fatal(err)
	}
	expectedMetaList := []marshal.BytesKV{
		{Key: []byte("b"), Value: []byte("value1")},
		{Key: []byte("c"), Value: []byte("value2")},
		{Key: []byte("d"), Value: []byte("value3")},
	}
	if !reflect.DeepEqual(metaList, expectedMetaList) {
		t.Errorf("Scan() returned unexpected value, expected: %v, got: %v", expectedMetaList, metaList)
	}
}

func TestBtreeIndexer_Delete(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(filepath.Join(getwd, uuid.New().String()+indexFileSuffixName))
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Remove()

	//insert
	tx, err := indexer.BeginTx()
	key := []byte("testKey")
	value := []byte("testValue")
	ops := make([]Op, 0)
	ops = append(ops, Op{Insert, marshal.BytesKV{Key: key, Value: value}})
	if err = indexer.ExecuteOps(tx, ops); err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	//delete
	tx, err = indexer.BeginTx()
	ops = make([]Op, 0)
	ops = append(ops, Op{Delete, marshal.BytesKV{Key: key, Value: value}})
	err = indexer.ExecuteOps(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	if _, err = indexer.Get(key); err != code.ErrRecordNotExists {
		t.Fatal(err)
	}
}
