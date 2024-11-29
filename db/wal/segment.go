package wal

import (
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db/iooperator"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"github.com/valyala/bytebufferpool"
	"io"
	"os"
	"path/filepath"
)

const (
	zero                = 0
	defaultMinLogIndex  = zero
	hSSegmentHeaderSize = 4
	persistIndexSize    = 8
	applyIndexSize      = 8
	applyTermSize       = 8
)

type segment struct {
	WalDirPath         string
	defaultSegmentSize int
	Index              uint64 //The minimum log index in this segment file.

	Fd        *os.File
	blockPool *blockPool

	blockNums     int //Record the number of blocks already allocated in the current segment.
	segmentOffset int //The offset written to the segment file.

	blocks           []byte //Blocks used by the current segment
	blocksOffset     int    //The offset of the current Blocks
	BlocksRemainSize int    //The remaining writable bytes in the current Blocks.

	closed bool
}

func segmentFileName(walDirPath string, index uint64) string {
	return filepath.Join(walDirPath, fmt.Sprintf("%014d"+SegSuffix, index))
}

func newSegmentFile(config config.WalConfig) *segment {
	fd, err := iooperator.OpenDirectIOFile(filepath.Join(config.WalDirPath, fmt.Sprintf("%s"+TMPSuffix, uuid.New().String())), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Panicf("create a new segment file error %v", err)
	}

	return &segment{
		WalDirPath:         config.WalDirPath,
		Index:              defaultMinLogIndex,
		Fd:                 fd,
		blockPool:          newBlockPool(),
		defaultSegmentSize: config.SegmentSize,
	}
}

func openOldSegmentFile(walDirPath string, index uint64) *segment {
	fd, err := iooperator.OpenDirectIOFile(segmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Panicf("open old segment file error %v", err)
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

func (seg *segment) write(data []byte, bytesCount int, firstIndex uint64) (err error) {
	// Reallocate blocks when Blocks is nil.
	if seg.blocks == nil {
		blocks, blockNums := seg.blockPool.alignedBlock(bytesCount)
		seg.BlocksRemainSize = blockNums * block4096
		seg.blocksOffset = 0
		seg.blocks = blocks
		seg.blockNums += blockNums
	}

	if bytesCount < seg.BlocksRemainSize {
		copy(seg.blocks[seg.blocksOffset:seg.blocksOffset+bytesCount], data)
	} else {
		// The current block is full, flush and allocate a new block.
		seg.segmentOffset += len(seg.blocks)
		seg.blockPool.recycleBlock(seg.blocks)

		newBlock, nums := seg.blockPool.alignedBlock(bytesCount)
		seg.BlocksRemainSize = nums * block4096
		seg.blockNums += nums
		seg.blocks = newBlock
		seg.blocksOffset = 0
		copy(seg.blocks[seg.blocksOffset:bytesCount], data)
	}

	if err = seg.flush(); err == nil {
		// Blocks that exceed Block4 or Block8 will not be recycled and will be directly cleared.
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

	//when first write data to segment update segment name
	if seg.Index == defaultMinLogIndex {
		if err = os.Rename(seg.Fd.Name(), segmentFileName(seg.WalDirPath, seg.Index)); err != nil {
			log.Panicf("open segment file %s failed: %v", seg.Fd.Name(), err)
			return err
		}
		seg.Index = firstIndex
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
	return seg.Fd.Close()
}

func (seg *segment) remove() error {
	return os.Remove(seg.Fd.Name())
}

// SegmentReader restore memory and truncate wal will use SegmentReader
type SegmentReader struct {
	blocks       []byte
	blocksOffset int // current read pointer in blocks
	blocksNums   int // blocks  number
}

func NewSegmentReader(seg *segment) *SegmentReader {
	var err error
	blocks := alignedCustomBlock(seg.blockNums)
	_, err = seg.Fd.Seek(zero, io.SeekStart)
	_, err = seg.Fd.Read(blocks)
	if err != nil {
		log.Panicf("new sgement reader failed , read file error: %v", err)
	}
	return &SegmentReader{
		blocks:     blocks,
		blocksNums: seg.blockNums,
	}
}

func (sr *SegmentReader) ReadHeader() (eHeader marshal.WalEntryHeader, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if _, err = buf.Write(sr.blocks[sr.blocksOffset : sr.blocksOffset+marshal.ChunkHeaderSize]); err != nil {
		return marshal.WalEntryHeader{}, err
	}
	eHeader = marshal.DecodeWALEntryHeader(buf.B)

	// If the current block has been read empty. It is necessary to judge whether data can be read from the next block. If it is empty, return EOF.
	if eHeader.IsEmpty() {
		blockNums := sr.blocksOffset / block4096
		if remain := sr.blocksOffset % block4096; remain > 0 {
			blockNums++
		}

		//current block is the last block in blocks，else read next block
		if len(sr.blocks)/block4096 == blockNums {
			return eHeader, io.EOF
		}

		//move pointer to next block, if next block is empty return eof
		sr.blocksOffset = blockNums * block4096
		copy(buf.B, sr.blocks[sr.blocksOffset:sr.blocksOffset+marshal.ChunkHeaderSize])
		eHeader = marshal.DecodeWALEntryHeader(buf.B)
		if eHeader.IsEmpty() {
			return eHeader, io.EOF
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
		log.Panicf("unmarshal entry failed %v ", err)
	}
	return
}

func (sr *SegmentReader) Next(entrySize int) {
	sr.blocksOffset += entrySize
}

// OrderedSegmentList Ordered single linked list composed of segments
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
		err := node.Seg.close()
		err = node.Seg.remove()
		if err != nil {
			log.Errorf("truncate segment file %s failed: %v", node.Seg.Fd.Name(), err)
		}
		node = node.Next
	}
}

type RaftStateSegment struct {
	fd        *os.File
	RaftState pb.HardState
	blocks    []byte
}

func openRaftStateSegment(fp string) (rSeg *RaftStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	rSeg = new(RaftStateSegment)
	rSeg.fd = fd
	rSeg.RaftState = pb.HardState{}
	rSeg.blocks = alignedCustomBlock(num4)
	fileInfo, _ := rSeg.fd.Stat()

	if fileInfo.Size() > 0 {
		if _, err = rSeg.fd.Read(rSeg.blocks); err != nil {
			return nil, err
		}
		rSeg.decodeRaftStateSegment()
	}

	return rSeg, nil
}

func (seg *RaftStateSegment) Save(hs pb.HardState) (err error) {
	seg.RaftState = hs
	data, err := seg.encodeRaftStateSegment()
	if err != nil {
		return err
	}
	copy(seg.blocks[0:len(data)], data)
	if _, err = seg.fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = seg.fd.Write(seg.blocks); err != nil {
		return err
	}
	return nil
}

func (seg *RaftStateSegment) encodeRaftStateSegment() ([]byte, error) {
	bytes, err := seg.RaftState.Marshal()
	if err != nil {
		return nil, err
	}
	nBytes := len(bytes)
	buf := make([]byte, nBytes+hSSegmentHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(nBytes))
	copy(buf[4:], bytes)
	return buf, nil
}

func (seg *RaftStateSegment) decodeRaftStateSegment() {
	header := int(binary.LittleEndian.Uint32(seg.blocks[0:4]))
	if err := seg.RaftState.Unmarshal(seg.blocks[4 : header+4]); err != nil {
		log.Panicf("unmarshal %v", err)
	}
	return
}

func (seg *RaftStateSegment) Close() error {
	return seg.fd.Close()
}

func (seg *RaftStateSegment) Remove() error {
	return os.Remove(seg.fd.Name())
}

type WalStateSegment struct {
	fd           *os.File
	AppliedIndex uint64
	AppliedTerm  uint64
	blocks       []byte
}

func OpenWalStateSegment(fp string) (kvSeg *WalStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	kvSeg = new(WalStateSegment)
	kvSeg.fd = fd
	kvSeg.blocks = alignedCustomBlock(num4)
	fileInfo, _ := kvSeg.fd.Stat()

	if fileInfo.Size() > 0 {
		kvSeg.fd.Read(kvSeg.blocks)
		kvSeg.decodeWalStateSegment()
	}

	return kvSeg, nil
}

func (seg *WalStateSegment) Save(appliedIndex, appliedTerm uint64) (err error) {
	if appliedIndex != seg.AppliedIndex && appliedIndex > 0 {
		seg.AppliedIndex = appliedIndex
		seg.AppliedTerm = appliedTerm
	}

	data := seg.encodeWalStateSegment()
	copy(seg.blocks[0:len(data)], data)
	if _, err = seg.fd.Seek(zero, io.SeekStart); err != nil {
		return
	}
	if _, err = seg.fd.Write(seg.blocks); err != nil {
		return
	}
	return nil
}

func (seg *WalStateSegment) encodeWalStateSegment() []byte {
	buf := make([]byte, applyIndexSize+applyTermSize)
	binary.LittleEndian.PutUint64(buf[zero:applyIndexSize], seg.AppliedIndex)
	binary.LittleEndian.PutUint64(buf[applyIndexSize:applyIndexSize+applyTermSize], seg.AppliedTerm)
	return buf
}

func (seg *WalStateSegment) decodeWalStateSegment() {
	seg.AppliedIndex = binary.LittleEndian.Uint64(seg.blocks[zero:applyIndexSize])
	seg.AppliedTerm = binary.LittleEndian.Uint64(seg.blocks[applyIndexSize : applyIndexSize+applyTermSize])
	return
}

func (seg *WalStateSegment) Close() error {
	return seg.fd.Close()
}

func (seg *WalStateSegment) Remove() error {
	return os.Remove(seg.fd.Name())
}

type VlogStateSegment struct {
	fd           *os.File
	PersistIndex uint64
	blocks       []byte
}

func OpenVlogStateSegment(fp string) (kvSeg *VlogStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	kvSeg = new(VlogStateSegment)
	kvSeg.fd = fd
	kvSeg.blocks = alignedCustomBlock(num4)
	fileInfo, _ := kvSeg.fd.Stat()

	if fileInfo.Size() > 0 {
		_, err = kvSeg.fd.Read(kvSeg.blocks)
		kvSeg.decodeVlogStateSegment()
	}

	return kvSeg, err
}

func (seg *VlogStateSegment) Save(persistIndex uint64) (err error) {
	if persistIndex != seg.PersistIndex && persistIndex > 0 {
		seg.PersistIndex = persistIndex
	}

	data := seg.encodeVlogStateSegment()
	copy(seg.blocks[zero:len(data)], data)
	if _, err = seg.fd.Seek(zero, io.SeekStart); err != nil {
		return err
	}
	if _, err = seg.fd.Write(seg.blocks); err != nil {
		return err
	}
	return nil
}

func (seg *VlogStateSegment) encodeVlogStateSegment() []byte {
	buf := make([]byte, persistIndexSize)
	binary.LittleEndian.PutUint64(buf[zero:persistIndexSize], seg.PersistIndex)
	return buf
}

func (seg *VlogStateSegment) decodeVlogStateSegment() {
	seg.PersistIndex = binary.LittleEndian.Uint64(seg.blocks[zero:persistIndexSize])
	return
}

func (seg *VlogStateSegment) Close() error {
	return seg.fd.Close()
}

func (seg *VlogStateSegment) Remove() error {
	return os.Remove(seg.fd.Name())
}
