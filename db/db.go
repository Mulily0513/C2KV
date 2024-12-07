package db

import (
	"errors"
	"github.com/Mulily0513/C2KV/code"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/db/wal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/utils"
	"io"
	"os"
	"sync"
)

//  log structure
//  ......persist................applied|first.................commit.....................stable....................last
//	--------|--------mem-table----------|--------------------storage slice------------------|-----raft log slice------|
//	--vlog--|--------------------------wal--------------------------------------------------|

//go:generate mockgen -source=./db.go -destination=./mocks/db.go -package=mocks
type Storage interface {
	Get(key []byte) (kv *marshal.KV, err error)
	Scan(lowKey []byte, highKey []byte) (kvs []*marshal.KV, err error)
	Apply(kvs []*marshal.KV) error

	PersistHardState(hs pb.HardState, wg *sync.WaitGroup)
	InitialState() (hs pb.HardState)

	PersistUnstableEnts(entries []*pb.Entry, wg *sync.WaitGroup)
	Entries(lo, hi uint64) ([]*pb.Entry, error)
	Term(i uint64) (uint64, error)
	FirstIndex() uint64
	AppliedIndex() uint64
	AppliedTerm() uint64
	StableIndex() uint64
	Truncate(index uint64) error

	Close()
	Remove()
}

type C2KV struct {
	dbPath string

	activeMem *memTable

	immtableQ *memTableQueue

	memFlushC chan *memTable

	memTablePipe chan *memTable

	wal *wal.WAL

	valueLog *ValueLog

	//stable raft log entries
	entries []*pb.Entry
}

func dbCfgCheck(dbCfg *config.DBConfig) {
	var err error
	if !utils.PathExist(dbCfg.DBPath) {
		if err = os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			log.Panicf("can not creat db directory err:%v", err)
		}
	}
	if !utils.PathExist(dbCfg.WalConfig.WalDirPath) {
		if err = os.MkdirAll(dbCfg.WalConfig.WalDirPath, os.ModePerm); err != nil {
			log.Panicf("can not creat wal directory err:%v", err)
		}
	}
	if !utils.PathExist(dbCfg.ValueLogConfig.ValueLogDir) {
		if err = os.MkdirAll(dbCfg.ValueLogConfig.ValueLogDir, os.ModePerm); err != nil {
			log.Panicf("can not creat vlog directory err:%v", err)
		}
	}
}

func OpenKVStorage(dbCfg *config.DBConfig) (C2 *C2KV) {
	dbCfgCheck(dbCfg)
	C2 = new(C2KV)
	C2.dbPath = dbCfg.DBPath
	memFlushC := make(chan *memTable, dbCfg.MemConfig.MemTableNums)
	C2.memTablePipe = make(chan *memTable, dbCfg.MemConfig.MemTablePipeSize)
	C2.immtableQ = newMemTableQueue(dbCfg.MemConfig.MemTableNums)
	C2.activeMem = newMemTable(dbCfg.MemConfig)
	C2.memFlushC = memFlushC
	C2.entries = make([]*pb.Entry, 0)
	C2.wal = wal.NewWal(dbCfg.WalConfig)

	go func() {
		for {
			C2.memTablePipe <- newMemTable(dbCfg.MemConfig)
		}
	}()

	//restore memory data
	if C2.wal.OrderSegmentList.Head != nil {
		go C2.restoreMemEntries()
		go C2.restoreImMemTable()
	}

	C2.valueLog = openValueLog(dbCfg.ValueLogConfig, memFlushC, C2.wal.VlogStateSegment)
	go C2.valueLog.listenAndFlush()
	return C2
}

//persistIndex............AppliedIndex.....committedIndex.......stableIndex......
//    |_________imm-table_________|___________ entries______________|

func (db *C2KV) restoreImMemTable() {
	persistIndex := db.wal.VlogStateSegment.PersistIndex
	appliedIndex := db.wal.WalStateSegment.AppliedIndex
	Node := db.wal.OrderSegmentList.Head
	kvC := make(chan *marshal.KV, 1000)
	var bytesCount int64
	errC := make(chan error)
	signalC := make(chan any)
	memTable := <-db.memTablePipe

	for Node != nil {
		if Node.Seg.Index <= persistIndex && Node.Next.Seg.Index > persistIndex {
			break
		}
		Node = Node.Next
	}

	go func() {
	Loop:
		for Node != nil {
			Seg := Node.Seg
			reader := wal.NewSegmentReader(Seg)
			for {
				header, err := reader.ReadHeader()
				if err == io.EOF {
					break
				}

				if header.Index < persistIndex {
					reader.Next(header.EntrySize)
					continue
				}
				if header.Index > appliedIndex {
					break Loop
				}

				ent, err := reader.ReadEntry(header)
				if err != nil {
					log.Panicf("read entry failed %v ", err)
				}

				kv := marshal.DecodeKV(ent.Data)
				kvC <- kv
				reader.Next(header.EntrySize)
			}
			Node = Node.Next
		}
		close(signalC)
		close(kvC)
	}()

	for {
		select {
		case kv := <-kvC:
			dataBytes := marshal.EncodeData(kv.Data)
			bytesCount += int64(len(dataBytes) + len(kv.Key))
			db.maybeRotateMemTable(bytesCount)
			if err := memTable.Put(&marshal.BytesKV{Key: kv.Key, Value: dataBytes}); err != nil {
				return
			}
		case err := <-errC:
			log.Panicf("read kv failed %v", err)
			return
		case <-signalC:
			for kv := range kvC {
				dataBytes := marshal.EncodeData(kv.Data)
				bytesCount += int64(len(dataBytes) + len(kv.Key))
				db.maybeRotateMemTable(bytesCount)
				if err := memTable.Put(&marshal.BytesKV{Key: kv.Key, Value: dataBytes}); err != nil {
					return
				}
			}
			return
		}
	}
}

func (db *C2KV) restoreMemEntries() {
	appliedIndex := db.wal.WalStateSegment.AppliedIndex
	committedIndex := db.wal.RaftStateSegment.RaftState.Commit
	Node := db.wal.OrderSegmentList.Head
	for Node != nil {
		if Node.Seg.Index <= appliedIndex && Node.Next.Seg.Index >= appliedIndex {
			break
		}
		Node = Node.Next
	}

ExitLoop:
	for Node != nil {
		ents := make([]*pb.Entry, 0)
		Seg := Node.Seg
		reader := wal.NewSegmentReader(Seg)
		for {
			header, err := reader.ReadHeader()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				log.Panicf("read header failed %v", err)
			}
			if header.Index < appliedIndex {
				reader.Next(header.EntrySize)
				continue
			}
			if header.Index > committedIndex {
				//todo Should the logs after the committedIndex be truncated?
				break ExitLoop
			}

			ent, err := reader.ReadEntry(header)
			if err != nil {
				log.Panicf("read entry failed %v", err)
			}

			ents = append(ents, ent)
			reader.Next(header.EntrySize)
		}

		db.entries = append(db.entries, ents...)
		Node = Node.Next
	}
	return
}

func (db *C2KV) Get(key []byte) (kv *marshal.KV, err error) {
	kv, flag := db.activeMem.Get(key)
	if !flag {
		for _, immem := range db.immtableQ.All() {
			kv, flag = immem.Get(key)
			if flag {
				return kv, nil
			}
		}
		return db.valueLog.Get(key)
	}
	return kv, nil
}

func (db *C2KV) Scan(lowKey []byte, highKey []byte) ([]*marshal.KV, error) {
	kvSlice := make([]*marshal.KV, 0)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		kvs, err := db.valueLog.Scan(lowKey, highKey)
		if err != nil {
			return
		}
		kvSlice = append(kvSlice, kvs...)
		wg.Done()

	}()

	var medbKvs []*marshal.KV
	for _, mem := range db.immtableQ.All() {
		memKvs, err := mem.Scan(lowKey, highKey)
		if err != nil {
			return nil, err
		}
		medbKvs = append(medbKvs, memKvs...)
	}
	activeMemKvs, err := db.activeMem.Scan(lowKey, highKey)
	if err != nil {
		return nil, err
	}
	medbKvs = append(medbKvs, activeMemKvs...)
	wg.Wait()
	kvSlice = append(kvSlice, medbKvs...)
	return kvSlice, nil
}

func (db *C2KV) Apply(kvs []*marshal.KV) (err error) {
	lastIndex := kvs[len(kvs)-1].Data.Index
	firstIndex := kvs[0].Data.Index
	if firstIndex != db.FirstIndex() {
		log.Panicf("the first index of kvs is not equal to the first index of wal %v", err)
	}

	kvBytes := make([]*marshal.BytesKV, len(kvs))
	var bytesCount int64
	for i, kv := range kvs {
		dataBytes := marshal.EncodeData(kv.Data)
		bytesCount += int64(len(dataBytes) + len(kv.Key))
		kvBytes[i] = &marshal.BytesKV{Key: kv.Key, Value: dataBytes}
	}
	db.maybeRotateMemTable(bytesCount)

	offset := int64(lastIndex) - int64(firstIndex) + 1
	if err = db.activeMem.ConcurrentPut(kvBytes); err != nil {
		return err
	}
	if err = db.wal.WalStateSegment.Save(lastIndex, db.entries[offset-1].Term); err != nil {
		return err
	}
	db.entries = db.entries[offset:]
	return
}

func (db *C2KV) maybeRotateMemTable(bytesCount int64) {
	if bytesCount+db.activeMem.Size() > db.activeMem.cfg.MemTableSize {
		if db.immtableQ.size > db.immtableQ.capacity/2 {
			db.memFlushC <- db.immtableQ.Dequeue()
		}
		db.immtableQ.Enqueue(db.activeMem)
		db.activeMem = <-db.memTablePipe
	}
}

func (db *C2KV) PersistHardState(st pb.HardState, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := db.wal.RaftStateSegment.Save(st); err != nil {
		log.Panicf("save raft hardstate failed")
	}
}

func (db *C2KV) InitialState() pb.HardState {
	return db.wal.RaftStateSegment.RaftState
}

func (db *C2KV) Truncate(index uint64) error {
	offset := db.entries[0].Index
	db.entries = db.entries[index-offset:]
	return db.wal.Truncate(index)
}

func (db *C2KV) PersistUnstableEnts(entries []*pb.Entry, wg *sync.WaitGroup) {
	defer wg.Done()

	if len(entries) == 0 {
		return
	}
	if err := db.wal.Write(entries); err != nil {
		log.Panicf("wal save unstable ents failed")
	}
	db.entries = append(db.entries, entries...)
}

func (db *C2KV) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	if lo < db.FirstIndex() || hi > db.lastIndex()+1 {
		return nil, errors.New("some entries is compacted")
	}

	offset := db.entries[0].Index
	ents := db.entries[lo-offset : hi-offset]

	return ents, nil
}

func (db *C2KV) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	if i < db.FirstIndex() {
		return 0, code.ErrCompacted
	}
	if len(db.entries) == 0 {
		return 0, code.ErrCompacted
	}
	offset := db.entries[0].Index
	return db.entries[i-offset].Term, nil
}

func (db *C2KV) FirstIndex() uint64 {
	if len(db.entries) == 0 {
		return 0
	}
	return db.entries[0].Index
}

func (db *C2KV) StableIndex() uint64 {
	return db.lastIndex()
}

func (db *C2KV) lastIndex() uint64 {
	if len(db.entries) == 0 {
		return 0
	}
	return db.entries[len(db.entries)-1].Index
}

func (db *C2KV) AppliedIndex() uint64 {
	return db.wal.WalStateSegment.AppliedIndex
}

func (db *C2KV) AppliedTerm() uint64 {
	return db.wal.WalStateSegment.AppliedTerm
}

func (db *C2KV) flushAllMemTable() {
	//todo
}

func (db *C2KV) Close() {
	db.flushAllMemTable()

	if err := db.wal.Close(); err != nil {
		log.Errorf("close wal failed %v", err)
	}

	if err := db.valueLog.Close(); err != nil {
		log.Errorf("close vlog failed %v", err)
	}
}

func (db *C2KV) Remove() {
	if err := os.RemoveAll(db.dbPath); err != nil {
		log.Errorf("remove db failed %v", err)
	}
}
