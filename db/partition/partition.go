package partition

import (
	"github.com/Mulily0513/C2KV/code"
	"github.com/Mulily0513/C2KV/db/iooperator"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/google/uuid"
	"github.com/valyala/bytebufferpool"
	"go.etcd.io/bbolt"
	"hash/crc32"
	"io"
	"os"
	"path"
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

type SST struct {
	Id      uint64
	fd      *os.File
	fName   string
	SSTSize int64
}

// OpenSST todo 一个value如果跨两个block，那么可能需要访问两次硬盘,后续优化vlog中数据的对齐
func OpenSST(filePath string) (*SST, error) {
	return &SST{
		Id:    uint64(uuid.New().ID()),
		fd:    iooperator.OpenBufferIOFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666),
		fName: filePath,
	}, nil
}

func (s *SST) Write(buf []byte) (err error) {
	_, err = s.fd.Write(buf)
	if err != nil {
		return err
	}
	return
}

func (s *SST) Read(vSize, vOffset int64) (buf []byte, err error) {
	_, err = s.fd.Seek(vOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	buf = make([]byte, vSize)
	_, err = s.fd.Read(buf)
	if err != nil {
		return nil, err
	}
	return
}

func (s *SST) Close() {
	if s.fd != nil {
		s.fd.Close()
	}
}

func (s *SST) Remove() {
	os.RemoveAll(s.fName)
}

func (s *SST) Rename(fName string) {
	if s.fd == nil {
		log.Panicf("fd is nil")
	}
	s.fd.Close()
	os.Rename(s.fName, fName)
	s.fd = iooperator.OpenBufferIOFile(fName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	s.fName = fName
}

func createSSTFileName(partitionDirPath string, Flag int, fileName string) string {
	switch Flag {
	case TmpSST:
		return path.Join(partitionDirPath, uuid.New().String()+SSTFileTmpSuffixName)
	case NewSST:
		return path.Join(partitionDirPath, uuid.New().String()+SSTFileSuffixName)
	case OldSST:
		return path.Join(partitionDirPath, fileName)
	default:
		return ""
	}
}

// Partition 一个partition文件由一个index文件和多个sst文件组成
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

	for _, file := range files {
		fName := file.Name()
		switch {
		case strings.HasSuffix(fName, indexFileSuffixName):
			p.indexer, err = NewIndexer(p.dirPath, fName)
		case strings.HasSuffix(fName, SSTFileSuffixName):
			sst, err := OpenSST(createSSTFileName(p.dirPath, OldSST, fName))
			if err != nil {
				return nil
			}
			p.SSTMap[sst.Id] = sst
		}
	}

	if p.indexer == nil {
		p.indexer, err = NewIndexer(p.dirPath, uuid.New().String()+indexFileSuffixName)
	}

	go func() {
		sst, err := OpenSST(createSSTFileName(p.dirPath, TmpSST, None))
		if err != nil {
			return
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
	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
		wg.Done()
		if err != nil {
			errC <- err
		}
	}()

	sst := <-p.pipeSST
	ops := make([]*Op, 0)
	var fileCurrentOffset int64
	for _, kv := range kvs {
		if kv.Data.Type == marshal.TypeDelete {
			ops = append(ops, &Op{Delete, marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value}})
			continue
		}

		vSize := len(kv.Data.Value)
		meta := &marshal.IndexerMeta{
			SSTid:       sst.Id,
			ValueOffset: fileCurrentOffset,
			ValueSize:   int64(vSize),
			ValueCrc32:  crc32.ChecksumIEEE(kv.Data.Value),
			TimeStamp:   kv.Data.TimeStamp,
		}

		//todo 小value直接存储在叶子节点中
		//if vSize <= smallValue {
		//	meta.Value = kv.Data.Value
		//}

		ops = append(ops, &Op{op: Insert, kv: marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeIndexMeta(meta)}})
		fileCurrentOffset += int64(len(kv.Data.Value))
		if _, err = buf.Write(kv.Data.Value); err != nil {
			return
		}
	}

	if tx, err = p.indexer.BeginTx(); err != nil {
		log.Errorf("start index transaction failed", err)
		return
	}
	if err = p.indexer.ExecuteOps(tx, ops); err != nil {
		tx.Rollback()
		return
	}
	if err = sst.Write(buf.Bytes()); err != nil {
		tx.Rollback()
		sst.Remove()
		return
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		sst.Remove()
		return
	}

	sst.Rename(createSSTFileName(p.dirPath, NewSST, None))
	p.SSTMap[sst.Id] = sst
}

func (p *Partition) AutoCompaction() {
	//todo compaction策略
	p.Compaction()
}

func (p *Partition) Compaction() {

}

func (p *Partition) Remove() error {
	return os.RemoveAll(p.dirPath)
}
