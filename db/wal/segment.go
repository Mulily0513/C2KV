package wal

import (
	"errors"
	"fmt"
	"github.com/Mulily0513/C2KV/db/iooperator"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type SegmentID = uint32

const (
	DefaultMinLogIndex = 0
)

type segment struct {
	WalDirPath         string
	Index              uint64 //该segment文件中的最小log index
	defaultSegmentSize int
	Fd                 *os.File
	blockPool          *blockPool

	blocks        []byte //当前segment使用的blocks
	blockNums     int    //记录当前segment已分配的blocks数量
	segmentOffset int    //blocks写入segment文件的偏移量

	blocksOffset     int //当前Blocks的偏移量
	BlocksRemainSize int //当前Blocks剩余可以写字节数
	closed           bool
}

func newSegmentFile(dirPath string, segmentSize int) *segment {
	fd, err := iooperator.OpenDirectIOFile(filepath.Join(dirPath, fmt.Sprintf("%s"+TMPSuffix, uuid.New().String())), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Panicf("create a new segment file error", err)
	}

	blockPool := newBlockPool()
	return &segment{
		WalDirPath:         dirPath,
		Index:              DefaultMinLogIndex,
		Fd:                 fd,
		blockPool:          blockPool,
		defaultSegmentSize: segmentSize,
	}
}

func openOldSegmentFile(walDirPath string, index uint64) *segment {
	fd, err := iooperator.OpenDirectIOFile(segmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Panicf("open old segment file error", err)
	}

	fileInfo, _ := fd.Stat()
	fSize := fileInfo.Size()
	blockNums := fSize / block4096
	remain := fSize % block4096
	if remain > 0 {
		blockNums++
	}

	return &segment{
		Index:     index,
		Fd:        fd,
		blockNums: int(blockNums),
	}
}

func segmentFileName(walDirPath string, index uint64) string {
	return filepath.Join(walDirPath, fmt.Sprintf("%014d"+SegSuffix, index))
}

func (seg *segment) write(data []byte, bytesCount int, firstIndex uint64) (err error) {
	//当Blocks为nil时重新分配blocks
	if seg.blocks == nil {
		blocks, blockNums := seg.blockPool.alignedBlock(bytesCount)
		seg.BlocksRemainSize = blockNums * block4096
		seg.blocksOffset = 0
		seg.blocks = blocks
		seg.blockNums += blockNums
	}

	if bytesCount < seg.BlocksRemainSize {
		copy(seg.blocks[seg.blocksOffset:bytesCount], data)
	} else {
		seg.segmentOffset += len(seg.blocks)
		seg.blockPool.recycleBlock(seg.blocks)

		newBlock, nums := seg.blockPool.alignedBlock(bytesCount)
		seg.BlocksRemainSize = nums * block4096
		seg.blockNums = seg.blockNums + nums
		seg.blocks = newBlock
		seg.blocksOffset = 0
		copy(seg.blocks[seg.blocksOffset:bytesCount], data)
	}

	if err = seg.flush(); err == nil {
		//超过Block4或者Block8的块不会回收直接清空
		if len(seg.blocks) > block4 || len(seg.blocks) > block8 {
			seg.segmentOffset += len(seg.blocks)
			seg.blocks = nil
			seg.BlocksRemainSize = 0
		} else {
			seg.blocksOffset += bytesCount
			seg.BlocksRemainSize -= bytesCount
		}
	} else {
		return err
	}

	//修改名字
	if seg.Index == DefaultMinLogIndex {
		seg.Index = firstIndex
		seg.Fd.Close()
		if err = os.Rename(seg.Fd.Name(), segmentFileName(seg.WalDirPath, seg.Index)); err != nil {
			log.Panicf("open segment file %s failed: %v", seg.Fd.Name(), err)
			return err
		}
		if seg.Fd, err = iooperator.OpenDirectIOFile(segmentFileName(seg.WalDirPath, seg.Index), os.O_CREATE|os.O_RDWR, 0644); err != nil {
			log.Panicf("open segment file %s failed: %v", seg.Fd.Name(), err)
			return err
		}
	}

	return
}

func (seg *segment) flush() (err error) {
	_, err = seg.Fd.Seek(int64(seg.segmentOffset), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = seg.Fd.Write(seg.blocks)
	if err != nil {
		return err
	}

	return
}

func (seg *segment) allocatedSize() int {
	return seg.blockNums * block4096
}

func (seg *segment) close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.Fd.Close()
}

func (seg *segment) remove() error {
	if seg.closed {
		err := os.Remove(seg.Fd.Name())
		if err != nil {
			log.Errorf("remove segment file %s failed: %v", seg.Fd.Name(), err)
		}
	} else {
		if err := seg.close(); err == nil {
			err = os.Remove(seg.Fd.Name())
			if err != nil {
				log.Errorf("remove segment file %s failed: %v", seg.Fd.Name(), err)
			}
		} else {
			return err
		}
	}
	return nil
}

// OrderedSegmentList 由segment组成的有序单链表
type OrderedSegmentList struct {
	Head *Node
}

type Node struct {
	Seg  *segment
	Next *Node
}

func newOrderedSegmentList() *OrderedSegmentList {
	return &OrderedSegmentList{}
}

func (oll *OrderedSegmentList) insert(seg *segment) {
	newNode := &Node{Seg: seg}

	if oll.Head == nil || oll.Head.Seg.Index >= seg.Index {
		newNode.Next = oll.Head
		oll.Head = newNode
		return
	}

	current := oll.Head
	for current.Next != nil && current.Next.Seg.Index < seg.Index {
		current = current.Next
	}

	newNode.Next = current.Next
	current.Next = newNode
}

// Find find segment which segment.index<=index and next segment.index>index
func (oll *OrderedSegmentList) find(index uint64) *segment {
	current := oll.Head
	var prev *Node

	for current != nil && current.Seg.Index < index {
		prev = current
		current = current.Next
	}

	if current != nil && current.Seg.Index == index {
		return current.Seg
	}

	if prev != nil {
		return prev.Seg
	}

	return nil
}

func (oll *OrderedSegmentList) truncate(index uint64) {
	node := oll.Head
	prev := new(Node)
	for node != nil {
		if node.Seg.Index < index {
			prev = node
			node = node.Next
			continue
		}
		prev.Next = nil
		break
	}

	for node != nil {
		node.Seg.close()
		node.Seg.remove()
		node = node.Next
	}
}

// SegmentReader restore memory and truncate wal will use reader
type SegmentReader struct {
	blocks       []byte
	blocksOffset int // current read pointer in blocks
	blocksNums   int // blocks  number
}

func NewSegmentReader(seg *segment) *SegmentReader {
	blocks := alignedblock(seg.blockNums)
	seg.Fd.Seek(0, io.SeekStart)
	_, err := seg.Fd.Read(blocks)
	if err != nil {
		log.Panicf("read file error", err)
	}
	return &SegmentReader{
		blocks:     blocks,
		blocksNums: seg.blockNums,
	}
}

func (sr *SegmentReader) ReadHeader() (eHeader marshal.WalEntryHeader, err error) {
	// todo chunkHeaderSlice应该池化减少GC
	buf := make([]byte, marshal.ChunkHeaderSize)
	copy(buf, sr.blocks[sr.blocksOffset:sr.blocksOffset+marshal.ChunkHeaderSize])
	eHeader = marshal.DecodeWALEntryHeader(buf)

	if eHeader.IsEmpty() {
		//当前block已经读空，需要判断下一个block是否能读出数据若为空则返回EOF
		//算出当前的blocksOffset位于blocks中的第几个块若为blocks中的最后一个块返回eof,
		//若不为最后一个块则移动指针到下一个块读取header
		blockNums := sr.blocksOffset / block4096
		if remain := sr.blocksOffset % block4096; remain > 0 {
			blockNums++
		}

		//若当前块为blocks中的最后一块return EOF
		if len(sr.blocks)/block4096 == blockNums {
			return eHeader, errors.New("EOF")
		}

		sr.blocksOffset = blockNums * block4096
		copy(buf, sr.blocks[sr.blocksOffset:sr.blocksOffset+marshal.ChunkHeaderSize])
		eHeader = marshal.DecodeWALEntryHeader(buf)
		if eHeader.IsEmpty() {
			return eHeader, errors.New("EOF")
		}
		sr.blocksOffset += marshal.ChunkHeaderSize
		return
	}
	sr.blocksOffset += marshal.ChunkHeaderSize
	return
}

func (sr *SegmentReader) ReadEntry(header marshal.WalEntryHeader) (ent *pb.Entry, err error) {
	ent = new(pb.Entry)
	err = ent.Unmarshal(sr.blocks[sr.blocksOffset : sr.blocksOffset+header.EntrySize])
	if err != nil {
		log.Panicf("unmarshal", err)
	}
	return
}

func (sr *SegmentReader) Next(entrySize int) {
	sr.blocksOffset += entrySize
}

// RaftStateSegment raft相关需要持久化的状态

type RaftStateSegment struct {
	fd        *os.File
	RaftState pb.HardState
	blocks    []byte
	closed    bool
}

func openRaftStateSegment(walDirPath, fileName string) (rSeg *RaftStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(filepath.Join(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	rSeg = new(RaftStateSegment)
	rSeg.fd = fd
	rSeg.RaftState = pb.HardState{}
	rSeg.blocks = alignedblock(num4)
	fileInfo, _ := rSeg.fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		rSeg.fd.Read(rSeg.blocks)
		rSeg.decodeRaftStateSegment()
	}

	return rSeg, nil
}

func (seg *RaftStateSegment) encodeRaftStateSegment() []byte {
	bytes, err := seg.RaftState.Marshal()
	length := len(bytes)
	if err != nil {
		return nil
	}

	return nil
}

func (seg *RaftStateSegment) decodeRaftStateSegment() {
	return
}

func (seg *RaftStateSegment) Save(hs pb.HardState) (err error) {
	seg.RaftState = hs
	data := seg.encodeRaftStateSegment()
	copy(seg.blocks[0:len(data)], data)
	if _, err = seg.fd.Seek(0, io.SeekStart); err != nil {
		return
	}
	if _, err = seg.fd.Write(data); err != nil {
		return err
	}
	return nil
}

func (seg *RaftStateSegment) close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.fd.Close()
}

func (seg *RaftStateSegment) remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}
	return os.Remove(seg.fd.Name())
}

// KVStateSegment 存储引擎相关需要持久化的状态
type KVStateSegment struct {
	fd           *os.File
	lock         *sync.Mutex
	PersistIndex uint64
	AppliedIndex uint64
	blocks       []byte
	closed       bool
}

func OpenKVStateSegment(walDirPath, fileName string) (kvSeg *KVStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(filepath.Join(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	kvSeg = new(KVStateSegment)
	kvSeg.fd = fd
	kvSeg.blocks = alignedblock(num4)
	fileInfo, _ := kvSeg.fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		kvSeg.fd.Read(kvSeg.blocks)
		kvSeg.decodeKVStateSegment()
	}

	return kvSeg, nil
}

func (seg *KVStateSegment) encodeKVStateSegment() []byte {
	return nil
}

func (seg *KVStateSegment) decodeKVStateSegment() {
	return
}

func (seg *KVStateSegment) Save(persistIndex, appliedIndex uint64) (err error) {
	if persistIndex != seg.PersistIndex && persistIndex > 0 {
		seg.PersistIndex = persistIndex
	}
	if appliedIndex != seg.AppliedIndex && persistIndex > 0 {
		seg.AppliedIndex = appliedIndex
	}
	data := seg.encodeKVStateSegment()
	copy(seg.blocks[0:len(data)], data)
	_, err = seg.fd.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	_, err = seg.fd.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (seg *KVStateSegment) close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.fd.Close()
}

func (seg *KVStateSegment) remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}

	return os.Remove(seg.fd.Name())
}
