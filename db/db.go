package db

import (
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"os"
	"sync"
)

//go:generate mockgen -source=./db.go -destination=./mocks/db.go -package=mocks
type Storage interface {
	Get(key []byte) (kv *marshal.KV, err error)
	Scan(lowKey []byte, highKey []byte) (kvs []*marshal.KV, err error)
	Apply(kvs []*marshal.KV) error

	PersistUnstableEnts(entries []*pb.Entry) error
	PersistHardState(st pb.HardState, cs pb.ConfState) error
	InitialState() (pb.HardState, pb.ConfState)

	Entries(lo, hi uint64) ([]pb.Entry, error)
	Term(i uint64) (uint64, error)
	FirstIndex() uint64
	AppliedIndex() uint64
	StableIndex() uint64
	Truncate(index uint64) error

	Close()
}

type C2KV struct {
	dbCfg *config.DBConfig

	activeMem *MemTable

	immtableQ *MemTableQueue

	memFlushC chan *MemTable

	memTablePipe chan *MemTable

	wal *wal.WAL

	valueLog *ValueLog

	logOffset uint64

	sync.Mutex

	entries []*pb.Entry //stable raft log entries
}

func dbCfgCheck(dbCfg *config.DBConfig) (err error) {
	if !utils.PathExist(dbCfg.DBPath) {
		if err = os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return
		}
	}
	if !utils.PathExist(dbCfg.WalConfig.WalDirPath) {
		if err = os.MkdirAll(dbCfg.WalConfig.WalDirPath, os.ModePerm); err != nil {
			return
		}
	}
	if !utils.PathExist(dbCfg.ValueLogConfig.ValueLogDir) {
		if err = os.MkdirAll(dbCfg.ValueLogConfig.ValueLogDir, os.ModePerm); err != nil {
			return
		}
	}
	return nil
}

func OpenKVStorage(dbCfg *config.DBConfig) (C2 *C2KV) {
	var err error
	if err = dbCfgCheck(dbCfg); err != nil {
		log.Panicf("db config check failed", err)
	}
	C2 = new(C2KV)
	memFlushC := make(chan *MemTable, dbCfg.MemConfig.MemTableNums)
	C2.memTablePipe = make(chan *MemTable, dbCfg.MemConfig.MemTablePipeSize)
	C2.immtableQ = NewMemTableQueue(dbCfg.MemConfig.MemTableNums)
	C2.activeMem = NewMemTable(dbCfg.MemConfig)
	C2.memFlushC = memFlushC
	C2.entries = make([]pb.Entry, 0)
	if C2.wal, err = wal.NewWal(dbCfg.WalConfig); err != nil {
		log.Panicf("open wal failed", err)
	}

	if C2.wal.OrderSegmentList.Head != nil {
		go C2.restoreMemEntries()
		go C2.restoreImMemTable()
	}

	if C2.valueLog, err = OpenValueLog(dbCfg.ValueLogConfig, memFlushC, C2.wal.KVStateSegment); err != nil {
		log.Panicf("open Value log failed", err)
	}

	go func() {
		for {
			C2.memTablePipe <- NewMemTable(dbCfg.MemConfig)
		}
	}()
	return C2
}

//persistIndex............AppliedIndex.....committedIndex.......stableIndex......
//    |_________imm-table_________|___________ entries______________|

func (db *C2KV) restoreImMemTable() {
	persistIndex := db.wal.KVStateSegment.PersistIndex
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex
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
				if err != nil {
					if err.Error() == "EOF" {
						break
					}
					log.Panicf("read header failed", err)
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
					log.Panicf("read entry failed", err)
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
			log.Panicf("read kv failed", err)
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
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex
	committedIndex := db.wal.RaftStateSegment.CommittedIndex
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
				log.Panicf("read header failed", err)
			}
			if header.Index < appliedIndex {
				reader.Next(header.EntrySize)
				continue
			}
			if header.Index > committedIndex {
				//todo 是否应该truncate掉committedIndex之后的日志？
				break ExitLoop
			}

			ent, err := reader.ReadEntry(header)
			if err != nil {
				log.Panicf("read entry failed", err)
			}

			ents = append(ents, ent)
			reader.Next(header.EntrySize)
		}

		db.entries = append(db.entries, ents...)
		Node = Node.Next
	}
	return
}

// kv operate

func (db *C2KV) Get(key []byte) (kv *marshal.KV, err error) {
	kv, flag := db.activeMem.Get(key)
	if !flag {
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
	for _, mem := range db.immtableQ.tables {
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
	kvBytes := make([]*marshal.BytesKV, len(kvs))
	var bytesCount int64
	for i, kv := range kvs {
		dataBytes := marshal.EncodeData(kv.Data)
		bytesCount += int64(len(dataBytes) + len(kv.Key))
		kvBytes[i] = &marshal.BytesKV{Key: kv.Key, Value: dataBytes}
	}
	db.maybeRotateMemTable(bytesCount)
	return db.activeMem.ConcurrentPut(kvBytes)
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

func (db *C2KV) PersistUnstableEnts(entries []*pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	if db.lastIndex()+1 != db.firstIndex() {
		log.Panicf("should not happen")
	}

	err := db.wal.Write(entries)
	if err != nil {
		return err
	}
	db.entries = append(db.entries, entries...)
	return nil
}

func (db *C2KV) PersistHardState(st pb.HardState, cs pb.ConfState) error {
	db.wal.RaftStateSegment.RaftState = st
	return db.wal.RaftStateSegment.Flush()
}

func (db *C2KV) Truncate(index uint64) error {
	offset := db.entries[0].Index
	db.entries = db.entries[index-offset:]
	return db.wal.Truncate(index)
}

func (db *C2KV) Entries(lo, hi uint64) (entries []pb.Entry, err error) {
	if lo < db.firstIndex() || hi > db.lastIndex() {
		return nil, errors.New("some entries is compacted")
	}

	if hi > db.lastIndex() {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, db.lastIndex())
	}

	offset := db.entries[0].Index
	ents := db.entries[lo-offset : hi-offset]

	return ents, nil
}

func (db *C2KV) Term(i uint64) (uint64, error) {
	db.Lock()
	defer db.Unlock()
	offset := db.entries[0].Index
	if i < offset {
		return 0, code.ErrCompacted
	}
	if int(i-offset) >= len(db.entries) {
		return 0, code.ErrUnavailable
	}
	return db.entries[i-offset].Term, nil
}

func (db *C2KV) AppliedIndex() uint64 {
	db.Lock()
	defer db.Unlock()
	return db.firstIndex()
}

func (db *C2KV) FirstIndex() uint64 {
	db.Lock()
	defer db.Unlock()
	return db.firstIndex()
}

func (db *C2KV) firstIndex() uint64 {
	return db.entries[0].Index + 1
}

func (db *C2KV) StableIndex() uint64 {
	db.Lock()
	defer db.Unlock()
	return db.lastIndex()
}

func (db *C2KV) lastIndex() uint64 {
	return db.entries[uint64(len(db.entries))-1].Index
}

func (db *C2KV) InitialState() (pb.HardState, pb.ConfState) {
	return pb.HardState{}, pb.ConfState{}
}

func (db *C2KV) Close() {

}
