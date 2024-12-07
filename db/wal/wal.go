package wal

import (
	"fmt"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	segmentPipe      chan *segment
	activeSegment    *segment
	OrderSegmentList *OrderedSegmentList
	RaftStateSegment *RaftStateSegment
	VlogStateSegment *VlogStateSegment
	WalStateSegment  *WalStateSegment
	lock             *sync.Mutex
}

func NewWal(config config.WalConfig) *WAL {
	segmentPipe := make(chan *segment, 1)
	go func() {
		for {
			segmentPipe <- newSegmentFile(config)
		}
	}()

	wal := &WAL{
		walDirPath:       config.WalDirPath,
		segmentSize:      config.SegmentSize * MB,
		OrderSegmentList: newOrderedSegmentList(),
		activeSegment:    newSegmentFile(config),
		segmentPipe:      segmentPipe,
		lock:             new(sync.Mutex),
	}

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
			wal.OrderSegmentList.insert(openOldSegmentFile(wal.walDirPath, index))
		}

		if strings.HasSuffix(fName, RaftSuffix) {
			if wal.RaftStateSegment, err = openRaftStateSegment(filepath.Join(wal.walDirPath, fName)); err != nil {
				log.Panicf("open old raft state segment file error %v", err)
			}
		}

		if strings.HasSuffix(fName, WALSuffix) {
			if wal.WalStateSegment, err = OpenWalStateSegment(filepath.Join(wal.walDirPath, fName)); err != nil {
				log.Panicf("open old wal state segment file error %v", err)
			}
		}

		if strings.HasSuffix(fName, VlogSuffix) {
			if wal.VlogStateSegment, err = OpenVlogStateSegment(filepath.Join(wal.walDirPath, fName)); err != nil {
				log.Panicf("open old vlog state segment file error %v", err)
			}
		}
	}

	if wal.RaftStateSegment == nil {
		if wal.RaftStateSegment, err = openRaftStateSegment(filepath.Join(wal.walDirPath, uuid.New().String()+RaftSuffix)); err != nil {
			log.Panicf("create a new raft state segment file error %v", err)
		}
	}

	if wal.WalStateSegment == nil {
		if wal.WalStateSegment, err = OpenWalStateSegment(filepath.Join(wal.walDirPath, uuid.New().String()+WALSuffix)); err != nil {
			log.Panicf("create a new kv state segment file error %v", err)
		}
	}

	if wal.VlogStateSegment == nil {
		if wal.VlogStateSegment, err = OpenVlogStateSegment(filepath.Join(wal.walDirPath, uuid.New().String()+VlogSuffix)); err != nil {
			log.Panicf("create a new kv state segment file error %v", err)
		}
	}

	return wal
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
	newSegment := <-wal.segmentPipe
	wal.OrderSegmentList.insert(wal.activeSegment)
	wal.activeSegment = newSegment
}

func (wal *WAL) Write(entries []*pb.Entry) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	data := make([]byte, 0)
	bytesCount := 0

	for _, e := range entries {
		wEntBytes, n := marshal.EncodeWALEntry(e)
		data = append(data, wEntBytes...)
		bytesCount += n
	}

	// if current active segment file is full, create a new one.
	if wal.activeSegmentIsFull(bytesCount) {
		wal.rotateActiveSegment()
	}

	return wal.activeSegment.write(data, bytesCount, entries[0].Index)
}

// Truncate truncates all segments after the index including the current active segment.
func (wal *WAL) Truncate(index uint64) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.OrderSegmentList.insert(wal.activeSegment)
	wal.activeSegment = <-wal.segmentPipe

	seg := wal.OrderSegmentList.find(index)
	reader := NewSegmentReader(seg)
	for {
		header, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		reader.Next(header.EntrySize)
		if header.Index == index {
			err = seg.Fd.Truncate(int64(reader.blocksOffset))
			if err != nil {
				log.Errorf("Truncate segment file failed %v", err)
				return err
			}
			break
		}
	}

	wal.OrderSegmentList.truncate(index)
	return nil
}

func (wal *WAL) RaftState() {

}

func (wal *WAL) Close() error {
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
