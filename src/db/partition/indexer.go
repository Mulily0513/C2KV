package partition

import (
	"bytes"
	"errors"
	"github.com/Mulily0513/C2KV/src/code"
	"github.com/Mulily0513/C2KV/src/db/marshal"
	"go.etcd.io/bbolt"
	"os"
)

// bucket name for bolt db to store index data
var indexBucketName = []byte("index")

const (
	indexFileSuffixName = ".INDEX"
	Insert              = 1
	Delete              = 2
)

func NewIndexer(indexFp string) (*BtreeIndexer, error) {
	indexer, err := bbolt.Open(indexFp, 0600,
		&bbolt.Options{
			NoSync:          true,
			InitialMmapSize: 1024,
			FreelistType:    bbolt.FreelistMapType,
		},
	)
	if err != nil {
		return nil, err
	}

	tx, err := indexer.Begin(true)
	if err != nil {
		return nil, err
	}
	if _, err = tx.CreateBucketIfNotExists(indexBucketName); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &BtreeIndexer{index: indexer, Fp: indexFp}, nil
}

type BtreeIndexer struct {
	index *bbolt.DB
	Fp    string
}

type Op struct {
	op int8
	kv marshal.BytesKV
}

func (b *BtreeIndexer) ExecuteOps(tx *bbolt.Tx, ops []Op) (err error) {
	txBucket := tx.Bucket(indexBucketName)
	for _, op := range ops {
		switch op.op {
		case Delete:
			if err = txBucket.Delete(op.kv.Key); err != nil {
				return
			}
		case Insert:
			if err = txBucket.Put(op.kv.Key, op.kv.Value); err != nil {
				return
			}
		}
	}
	return
}

func (b *BtreeIndexer) BeginTx() (tx *bbolt.Tx, err error) {
	return b.index.Begin(true)
}

func (b *BtreeIndexer) Get(key []byte) (meta marshal.BytesKV, err error) {
	err = b.index.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(indexBucketName).Get(key)
		if value != nil {
			meta.Key = key
			meta.Value = value
			return nil
		}
		return code.ErrRecordNotExists
	})
	return
}

func (b *BtreeIndexer) Scan(low, high []byte) (meta []marshal.BytesKV, err error) {
	err = b.index.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(indexBucketName).Cursor()
		k, v := cursor.Seek(low)
		if k != nil && bytes.Compare(k, low) >= 0 {
			meta = append(meta, marshal.BytesKV{Key: k, Value: v})
		}
		for bytes.Compare(k, high) <= 0 {
			k, v = cursor.Next()
			if k == nil {
				break
			}
			meta = append(meta, marshal.BytesKV{Key: k, Value: v})
		}
		return nil
	})
	return
}

func (b *BtreeIndexer) Close() error {
	if b.index != nil {
		return b.index.Close()
	}
	return errors.New("indexer is not exist")
}

func (b *BtreeIndexer) Remove() error {
	return os.Remove(b.Fp)
}
