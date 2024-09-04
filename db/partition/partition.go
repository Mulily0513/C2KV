package partition

import (
	"github.com/Mulily0513/C2KV/code"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/google/uuid"
	"github.com/valyala/bytebufferpool"
	"go.etcd.io/bbolt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

const (
	SSTFileSuffixName    = ".SST"
	SSTFileTmpSuffixName = ".SST-TMP"
	smallValue           = 128
	OldSST               = 1
	NewSST               = 2
	TmpSST               = 3
	None                 = ""
)

func createSSTFilePath(partitionDirPath string, Flag int, fileName string) string {
	switch Flag {
	case TmpSST:
		return path.Join(partitionDirPath, uuid.New().String()+SSTFileTmpSuffixName)
	case NewSST:
		return path.Join(partitionDirPath, uuid.New().String()+SSTFileSuffixName)
	case OldSST:
		return path.Join(partitionDirPath, fileName)
	default:
		log.Panicf("sst file flag is error %d", Flag)
		return ""
	}
}

// Partition  consists of an index file and multiple sst files.
type Partition struct {
	dirPath string
	pipeSST chan *SST
	indexer *BtreeIndexer
	SSTMap  map[uint64]*SST
}

func OpenPartition(partitionDir string) (p *Partition) {
	p = &Partition{
		dirPath: partitionDir,
		pipeSST: make(chan *SST, 1),
		SSTMap:  make(map[uint64]*SST),
	}

	files, err := os.ReadDir(partitionDir)
	if err != nil {
		log.Panicf("open partition dir failed %e", err)
	}

	var fName string
	for _, file := range files {
		fName = file.Name()
		switch {
		case strings.HasSuffix(fName, indexFileSuffixName):
			p.indexer, err = NewIndexer(filepath.Join(p.dirPath, fName))
		case strings.HasSuffix(fName, SSTFileSuffixName):
			sst, err := OpenSST(createSSTFilePath(p.dirPath, OldSST, fName))
			if err != nil {
				return nil
			}
			p.SSTMap[sst.Id] = sst
		}
	}

	if p.indexer == nil {
		p.indexer, err = NewIndexer(filepath.Join(p.dirPath, uuid.New().String()+indexFileSuffixName))
	}

	go func() {
		sst, err := OpenSST(createSSTFilePath(p.dirPath, TmpSST, None))
		if err != nil {
			log.Errorf("create tmp sst file failed %e", err)
		}
		p.pipeSST <- sst
	}()

	go p.AutoCompaction()
	return
}

func (p *Partition) Get(key []byte) (kv *marshal.KV, err error) {
	indexMeta, err := p.indexer.Get(key)
	if err != nil {
		return nil, err
	}
	index := marshal.DecodeIndexMeta(indexMeta.Value)

	if sst, ok := p.SSTMap[index.SSTid]; ok {
		value, err := sst.Read(index.ValueSize, index.ValueOffset)
		if err != nil {
			return nil, err
		}
		return &marshal.KV{
			Key:  key,
			Data: &marshal.Data{TimeStamp: index.TimeStamp, Value: value},
		}, nil
	}
	return nil, code.ErrCanNotFondSSTFile
}

func (p *Partition) Scan(low, high []byte) (kvs []*marshal.KV, err error) {
	indexMetas, err := p.indexer.Scan(low, high)
	if err != nil {
		return
	}
	for _, indexMeta := range indexMetas {
		index := marshal.DecodeIndexMeta(indexMeta.Value)
		if sst, ok := p.SSTMap[index.SSTid]; ok {
			value, err := sst.Read(index.ValueSize, index.ValueOffset)
			if err != nil {
				return nil, err
			}
			kvs = append(kvs, &marshal.KV{
				Key:  indexMeta.Key,
				Data: &marshal.Data{TimeStamp: index.TimeStamp, Value: value},
			})
		}
	}
	return
}

func (p *Partition) PersistKvs(kvs []*marshal.KV, wg *sync.WaitGroup, errC chan error) {
	var err error
	var tx *bbolt.Tx
	var sst *SST

	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
		if err != nil {
			tx.Rollback()
			sst.Remove()
			errC <- err
		}
		wg.Done()
	}()

	sst = <-p.pipeSST
	ops := make([]Op, 0)
	var vlogCurOffset int64
	for _, kv := range kvs {
		if kv.Data.Type == marshal.TypeDelete {
			ops = append(ops, Op{Delete, marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value}})
			continue
		}

		vSize := len(kv.Data.Value)
		meta := &marshal.IndexerMeta{
			SSTid:       sst.Id,
			ValueOffset: vlogCurOffset,
			ValueSize:   int64(vSize),
			ValueCrc32:  crc32.ChecksumIEEE(kv.Data.Value),
			TimeStamp:   kv.Data.TimeStamp,
		}
		//small value store in indexer
		if vSize <= smallValue {
			meta.Value = kv.Data.Value
			continue
		}

		ops = append(ops, Op{op: Insert, kv: marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeIndexMeta(meta)}})
		vlogCurOffset += int64(len(kv.Data.Value))
		if _, err = buf.Write(kv.Data.Value); err != nil {
			return
		}
	}

	if tx, err = p.indexer.BeginTx(); err != nil {
		return
	}

	if err = p.indexer.ExecuteOps(tx, ops); err != nil {
		return
	}

	if err = sst.Write(buf.Bytes()); err != nil {
		return
	}

	if err = tx.Commit(); err != nil {
		return
	}

	if err = sst.Rename(createSSTFilePath(p.dirPath, NewSST, None)); err != nil {
		return
	}
	p.SSTMap[sst.Id] = sst
}

func (p *Partition) AutoCompaction() {
	p.Compaction()
}

func (p *Partition) Compaction() {

}

func (p *Partition) Remove() error {
	for _, sst := range p.SSTMap {
		if err := sst.Remove(); err != nil {
			return err
		}
	}
	return p.indexer.Remove()
}

func (p *Partition) Close() error {
	for _, sst := range p.SSTMap {
		if err := sst.Close(); err != nil {
			return err
		}
	}
	for sst := range p.pipeSST {
		if err := sst.Remove(); err != nil {
			return err
		}
	}
	return p.indexer.Close()
}
