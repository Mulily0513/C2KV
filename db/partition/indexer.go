package partition

import (
	"bytes"
	"github.com/Mulily0513/C2KV/code"
	"github.com/Mulily0513/C2KV/db/marshal"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

// bucket name for bolt db to store index data
var indexBucketName = []byte("index")

const (
	indexFileSuffixName = ".INDEX"
	Insert              = 1
	Delete              = 2
)

func NewIndexer(partitionDir string, indexFileName string) (*BtreeIndexer, error) {
	fp := filepath.Join(partitionDir, indexFileName)
	indexer, err := bbolt.Open(fp, 0600,
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
	if _, err := tx.CreateBucketIfNotExists(indexBucketName); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &BtreeIndexer{
		index: indexer,
		Fp:    fp,
	}, nil
}

type BtreeIndexer struct {
	index *bbolt.DB
	Fp    string
}

type Op struct {
	op int8
	kv *marshal.BytesKV
}

func (b *BtreeIndexer) Get(key []byte) (meta *marshal.BytesKV, err error) {
	err = b.index.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(indexBucketName).Get(key)
		if value != nil {
			meta = new(marshal.BytesKV)
			meta.Key = key
			meta.Value = value
			return nil
		}
		return code.ErrRecordNotExists
	})
	return
}

func (b *BtreeIndexer) Scan(low, high []byte) (meta []*marshal.BytesKV, err error) {
	err = b.index.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(indexBucketName).Cursor()
		k, v := cursor.Seek(low)
		if k != nil && bytes.Compare(k, low) >= 0 {
			meta = append(meta, &marshal.BytesKV{Key: k, Value: v})
		}
		for bytes.Compare(k, high) <= 0 {
			k, v = cursor.Next()
			if k == nil {
				break
			}
			meta = append(meta, &marshal.BytesKV{Key: k, Value: v})
		}
		return nil
	})
	return
}

func (b *BtreeIndexer) Execute(tx *bbolt.Tx, ops []*Op) (err error) {
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

func (b *BtreeIndexer) StartTx() (tx *bbolt.Tx, err error) {
	return b.index.Begin(true)
}

func (b *BtreeIndexer) Close() error {
	return b.index.Close()
}

func (b *BtreeIndexer) Remove() error {
	return os.RemoveAll(b.Fp)
}
