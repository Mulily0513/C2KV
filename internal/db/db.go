package db

import (
	"encoding/binary"
	"errors"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/code"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/wal"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/Mulily0513/C2KV/internal/telemetry"
	"github.com/Mulily0513/C2KV/internal/utils"
	"io"
	"os"
	"sync"
	"time"
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

	mu sync.RWMutex
	// readySyncMu guards ready-stage sync context so we can aggregate sync
	// operations into logical barriers per Ready batch.
	readySyncMu sync.Mutex

	closeOnce sync.Once
	stopc     chan struct{}
	closed    bool

	activeMem *memTable

	immtableQ *memTableQueue

	memFlushC chan *memTable

	memTablePipe chan *memTable

	wal *wal.WAL

	valueLog *ValueLog

	//stable raft log entries
	entries []*pb.Entry

	readySyncActive    bool
	readyMustSync      bool
	readyWALDirty      bool
	readyRaftMustSync  bool
	readyWalStateDirty bool

	walStateCheckpointEvery uint64
	walStatePendingSyncs    uint64
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
	C2.stopc = make(chan struct{})
	C2.walStateCheckpointEvery = uint64(dbCfg.WalConfig.NormalizedWalStateCheckpoint())
	C2.wal = wal.NewWal(dbCfg.WalConfig)

	go func() {
		for {
			select {
			case <-C2.stopc:
				return
			case C2.memTablePipe <- newMemTable(dbCfg.MemConfig):
			}
		}
	}()

	// Restore memory state before raft starts to keep applied/entries boundaries consistent.
	if C2.wal.OrderSegmentList.Head != nil {
		C2.restoreMemEntries()
		C2.restoreImMemTable()
	}

	C2.valueLog = openValueLog(dbCfg.ValueLogConfig, memFlushC, C2.wal.VlogStateSegment)
	go C2.valueLog.listenAndFlush()
	return C2
}

func (db *C2KV) BeginReadySync(mustSync bool) {
	db.readySyncMu.Lock()
	defer db.readySyncMu.Unlock()
	db.readySyncActive = true
	db.readyMustSync = mustSync
	db.readyWALDirty = false
	db.readyRaftMustSync = false
	db.readyWalStateDirty = false
}

func (db *C2KV) SyncReadyBeforeSend() error {
	db.readySyncMu.Lock()
	active := db.readySyncActive
	mustSync := db.readyMustSync
	walDirty := db.readyWALDirty
	raftMustSync := db.readyRaftMustSync
	db.readySyncMu.Unlock()

	if !active || !mustSync {
		return nil
	}
	if !walDirty && !raftMustSync {
		return nil
	}

	if walDirty && !raftMustSync {
		return db.wal.SyncActiveSegment()
	}
	if raftMustSync && !walDirty {
		return db.wal.SyncRaftStateSegment()
	}

	var (
		wg      sync.WaitGroup
		walErr  error
		raftErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		walErr = db.wal.SyncActiveSegment()
	}()
	go func() {
		defer wg.Done()
		raftErr = db.wal.SyncRaftStateSegment()
	}()
	wg.Wait()
	if walErr != nil {
		return walErr
	}
	return raftErr
}

func (db *C2KV) FinishReadySync() error {
	db.readySyncMu.Lock()
	active := db.readySyncActive
	mustSync := db.readyMustSync
	walStateDirty := db.readyWalStateDirty
	shouldSyncWalState := false
	if active && mustSync && walStateDirty {
		db.walStatePendingSyncs++
		shouldSyncWalState = db.walStateCheckpointEvery <= 1 || db.walStatePendingSyncs >= db.walStateCheckpointEvery
	}
	db.readySyncActive = false
	db.readyMustSync = false
	db.readyWALDirty = false
	db.readyRaftMustSync = false
	db.readyWalStateDirty = false
	db.readySyncMu.Unlock()

	if !active || !mustSync {
		return nil
	}
	if walStateDirty && shouldSyncWalState {
		if err := db.wal.WalStateSegment.Save(
			db.wal.WalStateSegment.AppliedIndex,
			db.wal.WalStateSegment.AppliedTerm,
		); err != nil {
			return err
		}
		db.readySyncMu.Lock()
		db.walStatePendingSyncs = 0
		db.readySyncMu.Unlock()
	}
	return nil
}

func (db *C2KV) inReadySync() bool {
	db.readySyncMu.Lock()
	defer db.readySyncMu.Unlock()
	return db.readySyncActive
}

func (db *C2KV) readyMustSyncEnabled() bool {
	db.readySyncMu.Lock()
	defer db.readySyncMu.Unlock()
	return db.readySyncActive && db.readyMustSync
}

func (db *C2KV) markReadyWALDirty() {
	db.readySyncMu.Lock()
	defer db.readySyncMu.Unlock()
	if db.readySyncActive {
		db.readyWALDirty = true
	}
}

func (db *C2KV) markReadyRaftMustSync() {
	db.readySyncMu.Lock()
	defer db.readySyncMu.Unlock()
	if db.readySyncActive {
		db.readyRaftMustSync = true
	}
}

func (db *C2KV) markReadyWalStateDirty() {
	db.readySyncMu.Lock()
	defer db.readySyncMu.Unlock()
	if db.readySyncActive {
		db.readyWalStateDirty = true
	}
}

//persistIndex............AppliedIndex.....committedIndex.......stableIndex......
//    |_________imm-table_________|___________ entries______________|

func (db *C2KV) restoreImMemTable() {
	persistIndex := db.wal.VlogStateSegment.PersistIndex
	appliedIndex := db.wal.WalStateSegment.AppliedIndex
	Node := db.wal.OrderSegmentList.Head
	kvC := make(chan *marshal.KV, 1000)
	var bytesCount int64

	for Node != nil {
		if persistIndex < Node.Seg.Index {
			break
		}
		if Node.Seg.Index <= persistIndex && (Node.Next == nil || Node.Next.Seg.Index > persistIndex) {
			break
		}
		Node = Node.Next
	}

	go func() {
		defer close(kvC)
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
	}()

	for kv := range kvC {
		if kv == nil || kv.Data == nil {
			continue
		}
		dataBytes := marshal.EncodeData(kv.Data)
		bytesCount += int64(len(dataBytes) + len(kv.Key))
		db.mu.Lock()
		db.maybeRotateMemTable(bytesCount)
		if err := db.activeMem.Put(&marshal.BytesKV{Key: kv.Key, Value: dataBytes}); err != nil {
			db.mu.Unlock()
			return
		}
		db.mu.Unlock()
	}
	return
}

func (db *C2KV) restoreMemEntries() {
	appliedIndex := db.wal.WalStateSegment.AppliedIndex
	committedIndex := db.wal.RaftStateSegment.RaftState.Commit
	Node := db.wal.OrderSegmentList.Head
	for Node != nil {
		if appliedIndex < Node.Seg.Index {
			break
		}
		if Node.Seg.Index <= appliedIndex && (Node.Next == nil || Node.Next.Seg.Index >= appliedIndex) {
			break
		}
		Node = Node.Next
	}

	stop := false
	for Node != nil {
		ents := make([]*pb.Entry, 0)
		Seg := Node.Seg
		reader := wal.NewSegmentReader(Seg)
		for {
			header, err := reader.ReadHeader()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Panicf("read header failed %v", err)
			}
			if header.Index < appliedIndex {
				reader.Next(header.EntrySize)
				continue
			}
			if header.Index > committedIndex {
				// The current segment may already have collected valid entries.
				// Stop scanning after appending them.
				stop = true
				break
			}

			ent, err := reader.ReadEntry(header)
			if err != nil {
				log.Panicf("read entry failed %v", err)
			}

			ents = append(ents, ent)
			reader.Next(header.EntrySize)
		}

		db.mu.Lock()
		db.entries = append(db.entries, ents...)
		db.mu.Unlock()
		if stop {
			break
		}
		Node = Node.Next
	}
	return
}

func (db *C2KV) Get(key []byte) (kv *marshal.KV, err error) {
	db.mu.RLock()
	active := db.activeMem
	immems := db.immtableQ.All()
	db.mu.RUnlock()

	kv, flag := active.Get(key)
	if !flag {
		for _, immem := range immems {
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
	wg := &sync.WaitGroup{}
	errC := make(chan error, 1)
	vlogKVC := make(chan []*marshal.KV, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		kvs, err := db.valueLog.Scan(lowKey, highKey)
		if err != nil {
			errC <- err
			return
		}
		vlogKVC <- kvs
	}()

	db.mu.RLock()
	active := db.activeMem
	immems := db.immtableQ.All()
	db.mu.RUnlock()

	sources := make([][]*marshal.KV, 0, len(immems)+2)
	for _, mem := range immems {
		memKvs, err := mem.Scan(lowKey, highKey)
		if err != nil {
			return nil, err
		}
		if len(memKvs) > 0 {
			sources = append(sources, memKvs)
		}
	}
	activeMemKvs, err := active.Scan(lowKey, highKey)
	if err != nil {
		return nil, err
	}
	if len(activeMemKvs) > 0 {
		sources = append(sources, activeMemKvs)
	}
	wg.Wait()
	select {
	case err := <-errC:
		return nil, err
	default:
	}
	select {
	case vlogKvs := <-vlogKVC:
		if len(vlogKvs) > 0 {
			// vlogKvs is already key-sorted by ValueLog.Scan merge path.
			sources = append(sources, vlogKvs)
		}
	default:
	}
	return mergeSortedKVSources(sources, true), nil
}

func (db *C2KV) Apply(kvs []*marshal.KV) (err error) {
	if len(kvs) == 0 {
		return nil
	}
	applyStart := time.Now()
	defer func() {
		telemetry.ObserveApply(time.Since(applyStart))
	}()

	db.mu.Lock()
	if len(db.entries) == 0 {
		db.mu.Unlock()
		return nil
	}

	appliedIndex := db.wal.WalStateSegment.AppliedIndex
	firstMem := db.firstIndexLocked()
	lastMem := db.lastIndexLocked()

	trimFrom := 0
	for trimFrom < len(kvs) {
		if kvs[trimFrom] == nil {
			db.mu.Unlock()
			return errors.New("apply kv is nil")
		}
		idx := kvApplyIndex(kvs[trimFrom])
		if idx <= appliedIndex || idx < firstMem {
			trimFrom++
			continue
		}
		break
	}
	if trimFrom >= len(kvs) {
		db.mu.Unlock()
		return nil
	}

	trimTo := len(kvs)
	for trimTo > trimFrom {
		if kvs[trimTo-1] == nil {
			db.mu.Unlock()
			return errors.New("apply kv is nil")
		}
		if kvApplyIndex(kvs[trimTo-1]) <= lastMem {
			break
		}
		trimTo--
	}
	if trimTo <= trimFrom {
		db.mu.Unlock()
		return nil
	}

	expectFirst := appliedIndex + 1
	firstIndex := kvApplyIndex(kvs[trimFrom])
	if firstIndex != expectFirst {
		log.Warnf(
			"skip non-contiguous apply batch, first=%d expected=%d mem=[%d,%d] applied=%d",
			firstIndex,
			expectFirst,
			firstMem,
			lastMem,
			appliedIndex,
		)
		db.mu.Unlock()
		return nil
	}

	lastIndex := kvApplyIndex(kvs[trimTo-1])
	lastOffset := lastIndex - db.entries[0].Index
	lastTerm := db.entries[lastOffset].Term

	batchSize := trimTo - trimFrom
	kvBytesVal := make([]marshal.BytesKV, 0, batchSize)
	kvBytes := make([]*marshal.BytesKV, 0, batchSize)
	var bytesCount int64
	for _, kv := range kvs[trimFrom:trimTo] {
		dataBytes, dataErr := kvDataBytesForApply(kv)
		if dataErr != nil {
			db.mu.Unlock()
			return dataErr
		}
		bytesCount += int64(len(dataBytes) + len(kv.Key))
		kvBytesVal = append(kvBytesVal, marshal.BytesKV{Key: kv.Key, Value: dataBytes})
		kvBytes = append(kvBytes, &kvBytesVal[len(kvBytesVal)-1])
	}

	db.maybeRotateMemTable(bytesCount)
	targetMem := db.activeMem
	db.mu.Unlock()

	if batchSize == 1 {
		if err = targetMem.Put(kvBytes[0]); err != nil {
			return err
		}
	} else {
		if err = targetMem.ConcurrentPut(kvBytes); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.inReadySync() {
		// Update in-memory applied state first. Durable persistence is gated by
		// ready-stage sync/checkpoint in FinishReadySync.
		db.wal.WalStateSegment.AppliedIndex = lastIndex
		db.wal.WalStateSegment.AppliedTerm = lastTerm
		if db.readyMustSyncEnabled() {
			db.markReadyWalStateDirty()
			return nil
		}
		// For non-mustSync ready batches, preserve previous behavior by writing
		// applied state without forcing fsync.
		if err = db.wal.WalStateSegment.SaveNoSync(lastIndex, lastTerm); err != nil {
			return err
		}
		return nil
	}
	if err = db.wal.WalStateSegment.Save(lastIndex, lastTerm); err != nil {
		return err
	}
	return nil
}

func kvDataBytesForApply(kv *marshal.KV) ([]byte, error) {
	if kv == nil {
		return nil, errors.New("apply kv is nil")
	}
	if len(kv.DataBytes) != 0 {
		return kv.DataBytes, nil
	}
	if kv.Data == nil {
		return nil, errors.New("apply kv missing data payload")
	}
	return marshal.EncodeData(kv.Data), nil
}

func kvApplyIndex(kv *marshal.KV) uint64 {
	if kv == nil {
		return 0
	}
	if kv.Data != nil {
		return kv.Data.Index
	}
	if len(kv.DataBytes) >= marshal.IndexSize {
		return binary.LittleEndian.Uint64(kv.DataBytes[:marshal.IndexSize])
	}
	return 0
}

func (db *C2KV) maybeRotateMemTable(bytesCount int64) {
	if db.closed {
		return
	}
	limitBytes := int64(db.activeMem.cfg.MemTableSize) * MB
	if bytesCount+db.activeMem.Size() > limitBytes {
		if db.immtableQ.size > db.immtableQ.capacity/2 {
			db.memFlushC <- db.immtableQ.Dequeue()
		}
		db.immtableQ.Enqueue(db.activeMem)
		db.activeMem = <-db.memTablePipe
	}
}

func (db *C2KV) PersistHardState(st pb.HardState, wg *sync.WaitGroup) {
	defer wg.Done()
	if db.inReadySync() {
		prev := db.wal.RaftStateSegment.RaftState
		if err := db.wal.RaftStateSegment.SaveNoSync(st); err != nil {
			log.Panicf("save raft hardstate failed")
		}
		if st.Term != prev.Term || st.Vote != prev.Vote {
			db.markReadyRaftMustSync()
		}
		return
	}
	if err := db.wal.RaftStateSegment.Save(st); err != nil {
		log.Panicf("save raft hardstate failed")
	}
}

func (db *C2KV) InitialState() pb.HardState {
	return db.wal.RaftStateSegment.RaftState
}

func (db *C2KV) Truncate(index uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.entries) == 0 {
		return db.wal.Truncate(index)
	}

	offset := db.entries[0].Index
	if index <= offset {
		db.entries = db.entries[:0]
	} else {
		cut := index - offset
		if cut > uint64(len(db.entries)) {
			cut = uint64(len(db.entries))
		}
		db.entries = db.entries[:cut]
	}
	return db.wal.Truncate(index)
}

func (db *C2KV) PersistUnstableEnts(entries []*pb.Entry, wg *sync.WaitGroup) {
	defer wg.Done()

	if len(entries) == 0 {
		return
	}

	walWriteStart := time.Now()
	var err error
	if db.inReadySync() {
		err = db.wal.WriteNoSync(entries)
		db.markReadyWALDirty()
	} else {
		err = db.wal.Write(entries)
	}
	telemetry.ObserveWALWrite(time.Since(walWriteStart))
	if err != nil {
		log.Panicf("wal save unstable ents failed")
	}

	db.mu.Lock()
	db.entries = append(db.entries, entries...)
	db.mu.Unlock()
}

func (db *C2KV) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if lo < db.firstIndexLocked() || hi > db.lastIndexLocked()+1 {
		return nil, errors.New("some entries is compacted")
	}
	if len(db.entries) == 0 {
		return nil, nil
	}

	offset := db.entries[0].Index
	ents := db.entries[lo-offset : hi-offset]
	result := make([]*pb.Entry, len(ents))
	for i, ent := range ents {
		if ent == nil {
			continue
		}
		cloned := *ent
		if ent.Data != nil {
			cloned.Data = append([]byte(nil), ent.Data...)
		}
		result[i] = &cloned
	}

	return result, nil
}

func (db *C2KV) Term(i uint64) (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if i == 0 {
		return 0, nil
	}
	if i == db.wal.WalStateSegment.AppliedIndex {
		return db.wal.WalStateSegment.AppliedTerm, nil
	}
	if i < db.firstIndexLocked() {
		return 0, code.ErrCompacted
	}
	if i > db.lastIndexLocked() {
		return 0, code.ErrUnavailable
	}
	if len(db.entries) == 0 {
		return 0, code.ErrCompacted
	}
	offset := db.entries[0].Index
	return db.entries[i-offset].Term, nil
}

func (db *C2KV) FirstIndex() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.firstIndexLocked()
}

func (db *C2KV) firstIndexLocked() uint64 {
	if len(db.entries) == 0 {
		return db.wal.WalStateSegment.AppliedIndex + 1
	}
	return db.entries[0].Index
}

func (db *C2KV) StableIndex() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.lastIndexLocked()
}

func (db *C2KV) lastIndex() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.lastIndexLocked()
}

func (db *C2KV) lastIndexLocked() uint64 {
	if len(db.entries) == 0 {
		return db.wal.WalStateSegment.AppliedIndex
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
	db.closeOnce.Do(func() {
		db.mu.Lock()
		db.closed = true
		close(db.stopc)
		close(db.memFlushC)
		db.mu.Unlock()
	})

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
