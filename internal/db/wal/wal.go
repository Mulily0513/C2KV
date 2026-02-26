package wal

import (
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/google/uuid"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	FileModePerm = 0644
	SegSuffix    = ".SEG"
	TMPSuffix    = ".TMP"
	RaftSuffix   = ".RAFT-SEG"
	VlogSuffix   = ".VLOG-SEG"
	WALSuffix    = ".WAL-SEG"
	MB           = 2 * 1024 * 1024
)

type WAL struct {
	walDirPath  string
	segmentSize int
	syncPolicy  *syncPolicyConfig
	segmentCfg  config.WalConfig

	segmentPipe      chan *segment
	segmentStopC     chan struct{}
	segmentStopOnce  sync.Once
	segmentCreateWg  sync.WaitGroup
	activeSegment    *segment
	OrderSegmentList *OrderedSegmentList
	RaftStateSegment *RaftStateSegment
	VlogStateSegment *VlogStateSegment
	WalStateSegment  *WalStateSegment
	lock             *sync.Mutex
}

func NewWal(config config.WalConfig) *WAL {
	if err := os.MkdirAll(config.WalDirPath, 0o755); err != nil {
		log.Panicf("create wal dir error %v", err)
	}

	policy := newSyncPolicy(config)
	wal := &WAL{
		walDirPath:       config.WalDirPath,
		segmentSize:      config.SegmentSize * MB,
		syncPolicy:       policy,
		segmentCfg:       config,
		OrderSegmentList: newOrderedSegmentList(),
		activeSegment:    newSegmentFile(config),
		segmentPipe:      make(chan *segment, 1),
		segmentStopC:     make(chan struct{}),
		lock:             new(sync.Mutex),
	}
	wal.startSegmentPrecreate(config)

	files, err := os.ReadDir(wal.walDirPath)
	if err != nil {
		log.Panicf("read wal dir error %v", err)
	}

	var index uint64
	for _, file := range files {
		fName := file.Name()
		if strings.HasSuffix(fName, SegSuffix) {
			if _, err = fmt.Sscanf(fName, "%d.SEG", &index); err != nil {
				log.Panicf("scan segment file id error %v", err)
			}
			wal.OrderSegmentList.insert(openOldSegmentFile(wal.walDirPath, index, wal.syncPolicy))
		}

		if strings.HasSuffix(fName, RaftSuffix) {
			if wal.RaftStateSegment, err = openRaftStateSegment(filepath.Join(wal.walDirPath, fName), wal.syncPolicy); err != nil {
				log.Panicf("open old raft state segment file error %v", err)
			}
		}

		if strings.HasSuffix(fName, WALSuffix) {
			if wal.WalStateSegment, err = OpenWalStateSegment(filepath.Join(wal.walDirPath, fName), wal.syncPolicy); err != nil {
				log.Panicf("open old wal state segment file error %v", err)
			}
		}

		if strings.HasSuffix(fName, VlogSuffix) {
			if wal.VlogStateSegment, err = OpenVlogStateSegment(filepath.Join(wal.walDirPath, fName), wal.syncPolicy); err != nil {
				log.Panicf("open old vlog state segment file error %v", err)
			}
		}
	}

	if wal.RaftStateSegment == nil {
		if wal.RaftStateSegment, err = openRaftStateSegment(filepath.Join(wal.walDirPath, uuid.New().String()+RaftSuffix), wal.syncPolicy); err != nil {
			log.Panicf("create a new raft state segment file error %v", err)
		}
	}

	if wal.WalStateSegment == nil {
		if wal.WalStateSegment, err = OpenWalStateSegment(filepath.Join(wal.walDirPath, uuid.New().String()+WALSuffix), wal.syncPolicy); err != nil {
			log.Panicf("create a new kv state segment file error %v", err)
		}
	}

	if wal.VlogStateSegment == nil {
		if wal.VlogStateSegment, err = OpenVlogStateSegment(filepath.Join(wal.walDirPath, uuid.New().String()+VlogSuffix), wal.syncPolicy); err != nil {
			log.Panicf("create a new kv state segment file error %v", err)
		}
	}

	return wal
}

func (wal *WAL) startSegmentPrecreate(cfg config.WalConfig) {
	wal.segmentCreateWg.Add(1)
	go func() {
		defer wal.segmentCreateWg.Done()
		for {
			seg, err := tryNewSegmentFile(cfg)
			if err != nil {
				select {
				case <-wal.segmentStopC:
					return
				default:
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			select {
			case wal.segmentPipe <- seg:
			case <-wal.segmentStopC:
				_ = seg.close()
				_ = seg.remove()
				return
			}
		}
	}()
}

func (wal *WAL) stopSegmentPrecreate() {
	wal.segmentStopOnce.Do(func() {
		close(wal.segmentStopC)
		wal.segmentCreateWg.Wait()
		for {
			select {
			case seg := <-wal.segmentPipe:
				_ = seg.close()
				_ = seg.remove()
			default:
				return
			}
		}
	})
}

func (wal *WAL) activeSegmentIsFull(delta int) bool {
	// The segment size should be as uniform as possible so that looking up an entry can improve the efficiency of finding a certain entry.
	// The active segment size is calculated based on the currently allocated number of blocks, not the actual data occupied space.
	actSegSize := wal.activeSegment.allocatedSize()
	totalSize := actSegSize + delta

	// 1、total size > wal.segmentSize
	// 2、delta > wal.SegmentSize*0.5
	// 3. Memory space has been allocated.
	return totalSize > wal.segmentSize && float64(delta) > float64(wal.segmentSize)*0.5 && wal.activeSegment.blockNums != 0
}

func (wal *WAL) rotateActiveSegment() {
	var newSegment *segment
	select {
	case newSegment = <-wal.segmentPipe:
	default:
		var err error
		newSegment, err = tryNewSegmentFile(wal.segmentCfg)
		if err != nil {
			log.Panicf("create fallback segment failed: %v", err)
		}
		// Keep sync mode/checkpoint behavior consistent with current WAL policy.
		newSegment.syncPolicy = wal.syncPolicy
	}
	wal.OrderSegmentList.insert(wal.activeSegment)
	wal.activeSegment = newSegment
}

func (wal *WAL) Write(entries []*pb.Entry) error {
	return wal.writeWithSync(entries, true)
}

func (wal *WAL) WriteNoSync(entries []*pb.Entry) error {
	return wal.writeWithSync(entries, false)
}

func (wal *WAL) writeWithSync(entries []*pb.Entry, doSync bool) error {
	data, bytesCount, err := encodeWALEntriesBatch(entries)
	if err != nil {
		return err
	}

	wal.lock.Lock()
	defer wal.lock.Unlock()

	// if current active segment file is full, create a new one.
	if wal.activeSegmentIsFull(bytesCount) {
		wal.rotateActiveSegment()
	}

	if doSync {
		return wal.activeSegment.write(data, bytesCount, entries[0].Index)
	}
	return wal.activeSegment.writeNoSync(data, bytesCount, entries[0].Index)
}

// encodeWALEntriesBatch serializes entries directly into one contiguous WAL
// payload to avoid per-entry temporary allocations and repeated slice growth.
func encodeWALEntriesBatch(entries []*pb.Entry) ([]byte, int, error) {
	if len(entries) == 0 {
		return nil, 0, nil
	}
	totalSize := 0
	for _, ent := range entries {
		totalSize += marshal.ChunkHeaderSize + ent.Size()
	}

	buf := make([]byte, totalSize)
	offset := 0
	for _, ent := range entries {
		bodySize := ent.Size()
		headerStart := offset
		bodyStart := headerStart + marshal.ChunkHeaderSize
		bodyEnd := bodyStart + bodySize

		n, err := ent.MarshalToSizedBuffer(buf[bodyStart:bodyEnd])
		if err != nil {
			return nil, 0, err
		}
		if n != bodySize {
			return nil, 0, fmt.Errorf("wal entry marshal size mismatch, index=%d expected=%d got=%d", ent.Index, bodySize, n)
		}

		binary.LittleEndian.PutUint32(buf[headerStart+marshal.Crc32Size:headerStart+marshal.Crc32Size+marshal.EntrySize], uint32(bodySize))
		binary.LittleEndian.PutUint64(buf[headerStart+marshal.Crc32Size+marshal.EntrySize:bodyStart], ent.Index)
		crc := crc32.ChecksumIEEE(buf[bodyStart:bodyEnd])
		binary.LittleEndian.PutUint32(buf[headerStart:headerStart+marshal.Crc32Size], crc)

		offset = bodyEnd
	}
	return buf, offset, nil
}

func (wal *WAL) SyncActiveSegment() error {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	return wal.activeSegment.sync()
}

func (wal *WAL) SyncRaftStateSegment() error {
	return wal.RaftStateSegment.sync()
}

func (wal *WAL) SyncWalStateSegment() error {
	return wal.WalStateSegment.sync()
}

// Truncate truncates all segments after the index including the current active segment.
func (wal *WAL) Truncate(index uint64) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.OrderSegmentList.insert(wal.activeSegment)
	wal.activeSegment = <-wal.segmentPipe

	seg := wal.OrderSegmentList.find(index)
	if seg == nil {
		return nil
	}
	reader := NewSegmentReader(seg)
	for {
		header, err := reader.ReadHeader()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if header.Index == index {
			// Truncate from `index` (inclusive): keep entries with index < `index`.
			truncateOffset := reader.blocksOffset - marshal.ChunkHeaderSize
			err = seg.Fd.Truncate(int64(truncateOffset))
			if err != nil {
				log.Errorf("Truncate segment file failed %v", err)
				return err
			}
			if seg.syncPolicy != nil {
				if err = seg.syncPolicy.sync(seg.Fd); err != nil {
					return err
				}
			}
			break
		}
		reader.Next(header.EntrySize)
	}

	wal.OrderSegmentList.truncate(index)
	return nil
}

func (wal *WAL) RaftState() {

}

func (wal *WAL) Close() error {
	wal.stopSegmentPrecreate()

	node := wal.OrderSegmentList.Head
	for node != nil {
		err := node.Seg.close()
		if err != nil {
			return err
		}
		node = node.Next
	}
	// close the active segment file.
	return wal.activeSegment.close()
}

func (wal *WAL) Remove() error {
	wal.stopSegmentPrecreate()

	node := wal.OrderSegmentList.Head
	for node != nil {
		err := node.Seg.remove()
		if err != nil {
			return err
		}
		node = node.Next
	}

	wal.activeSegment.remove()

	filepath.Walk(wal.walDirPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(path, ".TMP") {
			os.Remove(path)
		}
		return nil
	})
	return nil
}
