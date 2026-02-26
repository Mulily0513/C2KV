package partition

import (
	"github.com/Mulily0513/C2KV/internal/code"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/google/uuid"
	"github.com/valyala/bytebufferpool"
	"go.etcd.io/bbolt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	SSTFileSuffixName     = ".SST"
	SSTFileTmpSuffixName  = ".SST-TMP"
	smallValue            = 128
	OldSST                = 1
	NewSST                = 2
	TmpSST                = 3
	None                  = ""
	compactionMinSST      = 32
	compactionInterval    = 5 * time.Second
	compactionQuietPeriod = 3 * time.Second
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

	mu                 sync.RWMutex
	compactionStopC    chan struct{}
	compactionStopOnce sync.Once
	compactionWg       sync.WaitGroup
	writeSeq           uint64
	lastWriteAt        time.Time
}

func OpenPartition(partitionDir string) (p *Partition) {
	p = &Partition{
		dirPath:         partitionDir,
		pipeSST:         make(chan *SST, 1),
		SSTMap:          make(map[uint64]*SST),
		compactionStopC: make(chan struct{}),
		lastWriteAt:     time.Now(),
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
	if err = p.prepareNextSST(); err != nil {
		log.Panicf("create tmp sst file failed %e", err)
	}

	p.compactionWg.Add(1)
	go p.AutoCompaction()
	return
}

func (p *Partition) prepareNextSST() error {
	sst, err := OpenSST(createSSTFilePath(p.dirPath, TmpSST, None))
	if err != nil {
		return err
	}
	p.pipeSST <- sst
	return nil
}

func (p *Partition) Get(key []byte) (kv *marshal.KV, err error) {
	indexMeta, err := p.indexer.Get(key)
	if err != nil {
		return nil, err
	}
	index := marshal.DecodeIndexMeta(indexMeta.Value)

	p.mu.RLock()
	defer p.mu.RUnlock()
	if sst, ok := p.SSTMap[index.SSTid]; ok {
		if len(index.Value) > 0 {
			return &marshal.KV{
				Key:  key,
				Data: &marshal.Data{TimeStamp: index.TimeStamp, Value: index.Value},
			}, nil
		}
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, indexMeta := range indexMetas {
		index := marshal.DecodeIndexMeta(indexMeta.Value)
		if sst, ok := p.SSTMap[index.SSTid]; ok {
			if len(index.Value) > 0 {
				kvs = append(kvs, &marshal.KV{
					Key:  indexMeta.Key,
					Data: &marshal.Data{TimeStamp: index.TimeStamp, Value: index.Value},
				})
				continue
			}
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
			if tx != nil {
				_ = tx.Rollback()
			}
			if sst != nil {
				_ = sst.Remove()
			}
			select {
			case errC <- err:
			default:
			}
		}
		wg.Done()
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	sst = <-p.pipeSST
	if err = p.prepareNextSST(); err != nil {
		return
	}
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
			ops = append(ops, Op{op: Insert, kv: marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeIndexMeta(meta)}})
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
	p.writeSeq++
	p.lastWriteAt = time.Now()
}

func (p *Partition) AutoCompaction() {
	defer p.compactionWg.Done()
	ticker := time.NewTicker(compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.Compaction()
		case <-p.compactionStopC:
			return
		}
	}
}

func (p *Partition) Compaction() {
	p.mu.RLock()
	if len(p.SSTMap) < compactionMinSST {
		p.mu.RUnlock()
		return
	}
	if !p.lastWriteAt.IsZero() && time.Since(p.lastWriteAt) < compactionQuietPeriod {
		p.mu.RUnlock()
		return
	}
	baseSeq := p.writeSeq
	sstSnapshot := make(map[uint64]*SST, len(p.SSTMap))
	for id, sst := range p.SSTMap {
		sstSnapshot[id] = sst
	}
	p.mu.RUnlock()

	metas, err := p.indexer.All()
	if err != nil {
		log.Errorf("partition compaction scan index failed: %v", err)
		return
	}
	if len(metas) == 0 {
		return
	}

	tmpSST, err := OpenSST(createSSTFilePath(p.dirPath, TmpSST, None))
	if err != nil {
		log.Errorf("partition compaction open tmp sst failed: %v", err)
		return
	}
	buf := bytebufferpool.Get()
	buf.Reset()
	defer bytebufferpool.Put(buf)

	ops := make([]Op, 0, len(metas))
	var offset int64
	for _, metaKV := range metas {
		indexMeta := marshal.DecodeIndexMeta(metaKV.Value)
		if len(indexMeta.Value) > 0 {
			continue
		}
		oldSST, ok := sstSnapshot[indexMeta.SSTid]
		if !ok {
			_ = tmpSST.Close()
			_ = tmpSST.Remove()
			log.Errorf("partition compaction missing old sst: %d", indexMeta.SSTid)
			return
		}
		value, readErr := oldSST.Read(indexMeta.ValueSize, indexMeta.ValueOffset)
		if readErr != nil {
			_ = tmpSST.Close()
			_ = tmpSST.Remove()
			log.Errorf("partition compaction read old sst failed: %v", readErr)
			return
		}
		if _, writeErr := buf.Write(value); writeErr != nil {
			_ = tmpSST.Close()
			_ = tmpSST.Remove()
			log.Errorf("partition compaction buffer write failed: %v", writeErr)
			return
		}
		newMeta := &marshal.IndexerMeta{
			SSTid:       tmpSST.Id,
			ValueOffset: offset,
			ValueSize:   int64(len(value)),
			ValueCrc32:  crc32.ChecksumIEEE(value),
			TimeStamp:   indexMeta.TimeStamp,
		}
		ops = append(ops, Op{
			op: Insert,
			kv: marshal.BytesKV{
				Key:   append([]byte(nil), metaKV.Key...),
				Value: marshal.EncodeIndexMeta(newMeta),
			},
		})
		offset += int64(len(value))
	}

	if len(ops) == 0 {
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.writeSeq != baseSeq {
			return
		}
		for id, old := range p.SSTMap {
			_ = old.Close()
			_ = old.Remove()
			delete(p.SSTMap, id)
		}
		return
	}
	if err = tmpSST.Write(buf.Bytes()); err != nil {
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		log.Errorf("partition compaction write tmp sst failed: %v", err)
		return
	}
	if err = tmpSST.Rename(createSSTFilePath(p.dirPath, NewSST, None)); err != nil {
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		log.Errorf("partition compaction rename sst failed: %v", err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writeSeq != baseSeq {
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		return
	}

	tx, err := p.indexer.BeginTx()
	if err != nil {
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		log.Errorf("partition compaction begin tx failed: %v", err)
		return
	}
	if err = p.indexer.ExecuteOps(tx, ops); err != nil {
		_ = tx.Rollback()
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		log.Errorf("partition compaction execute ops failed: %v", err)
		return
	}
	if err = tx.Commit(); err != nil {
		_ = tmpSST.Close()
		_ = tmpSST.Remove()
		log.Errorf("partition compaction commit failed: %v", err)
		return
	}

	for id, old := range p.SSTMap {
		if id == tmpSST.Id {
			continue
		}
		_ = old.Close()
		_ = old.Remove()
		delete(p.SSTMap, id)
	}
	p.SSTMap[tmpSST.Id] = tmpSST
}

func (p *Partition) Remove() error {
	p.stopCompaction()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, sst := range p.SSTMap {
		if err := sst.Remove(); err != nil {
			return err
		}
	}
	return p.indexer.Remove()
}

func (p *Partition) Close() error {
	p.stopCompaction()
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for _, sst := range p.SSTMap {
		if err := sst.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Drain temporary SST channel without blocking.
	for {
		select {
		case sst := <-p.pipeSST:
			if sst != nil {
				if err := sst.Remove(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		default:
			if err := p.indexer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			return firstErr
		}
	}
}

func (p *Partition) stopCompaction() {
	p.compactionStopOnce.Do(func() {
		close(p.compactionStopC)
	})
	p.compactionWg.Wait()
}
