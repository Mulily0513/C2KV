package wal

import (
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/iooperator"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/log"
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

	syncPolicy *syncPolicyConfig
}

func segmentFileName(walDirPath string, index uint64) string {
	return filepath.Join(walDirPath, fmt.Sprintf("%014d"+SegSuffix, index))
}

func newSegmentFile(config config.WalConfig) *segment {
	seg, err := tryNewSegmentFile(config)
	if err != nil {
		log.Panicf("create a new segment file error %v", err)
	}
	return seg
}

func tryNewSegmentFile(config config.WalConfig) (*segment, error) {
	fp := filepath.Join(config.WalDirPath, fmt.Sprintf("%s"+TMPSuffix, uuid.New().String()))
	fd, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &segment{
		WalDirPath:         config.WalDirPath,
		Index:              defaultMinLogIndex,
		Fd:                 fd,
		blockPool:          newBlockPool(),
		defaultSegmentSize: config.SegmentSize,
		syncPolicy:         newSyncPolicy(config),
	}, nil
}

func openOldSegmentFile(walDirPath string, index uint64, policies ...*syncPolicyConfig) *segment {
	fd := iooperator.OpenBufferIOFile(segmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)

	fileInfo, _ := fd.Stat()
	fSize := fileInfo.Size()
	blockNums := fSize / block4096
	remain := fSize % block4096
	if remain > 0 {
		blockNums++
	}

	seg := &segment{
		Index:         index,
		Fd:            fd,
		blockNums:     int(blockNums),
		segmentOffset: int(fSize),
	}
	if len(policies) > 0 {
		seg.syncPolicy = policies[0]
	}
	return seg
}

func (seg *segment) write(data []byte, bytesCount int, firstIndex uint64) (err error) {
	return seg.writeWithSync(data, bytesCount, firstIndex, true)
}

func (seg *segment) writeNoSync(data []byte, bytesCount int, firstIndex uint64) (err error) {
	return seg.writeWithSync(data, bytesCount, firstIndex, false)
}

func (seg *segment) writeWithSync(data []byte, bytesCount int, firstIndex uint64, doSync bool) (err error) {
	if bytesCount == 0 {
		return nil
	}

	// segmentOffset tracks the physical append position.
	if seg.segmentOffset == 0 {
		if fi, statErr := seg.Fd.Stat(); statErr == nil && fi.Size() > 0 {
			seg.segmentOffset = int(fi.Size())
		}
	}

	if err = seg.flush(data); err != nil {
		return err
	}
	seg.segmentOffset += bytesCount
	seg.blocksOffset += bytesCount
	seg.BlocksRemainSize = 0
	seg.blockNums = (seg.segmentOffset + block4096 - 1) / block4096
	seg.blocks = nil
	if doSync && seg.syncPolicy != nil {
		if err = seg.syncPolicy.sync(seg.Fd); err != nil {
			return err
		}
	}

	//when first write data to segment update segment name
	if seg.Index == defaultMinLogIndex {
		if err = os.Rename(seg.Fd.Name(), segmentFileName(seg.WalDirPath, firstIndex)); err != nil {
			log.Panicf("open segment file %s failed: %v", seg.Fd.Name(), err)
			return err
		}
		seg.Index = firstIndex
	}

	return
}

func (seg *segment) sync() error {
	if seg == nil || seg.Fd == nil || seg.syncPolicy == nil {
		return nil
	}
	return seg.syncPolicy.sync(seg.Fd)
}

func (seg *segment) flush(data []byte) error {
	n, err := seg.Fd.WriteAt(data, int64(seg.segmentOffset))
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
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
	if seg.blockNums <= 0 {
		return &SegmentReader{}
	}

	blocks := alignedCustomBlock(seg.blockNums)
	if _, err := seg.Fd.ReadAt(blocks, 0); err != nil && err != io.EOF {
		log.Panicf("new segment reader failed, read file error: %v", err)
	}
	return &SegmentReader{
		blocks:     blocks,
		blocksNums: seg.blockNums,
	}
}

func (sr *SegmentReader) ReadHeader() (eHeader marshal.WalEntryHeader, err error) {
	if len(sr.blocks) == 0 || sr.blocksOffset+marshal.ChunkHeaderSize > len(sr.blocks) {
		return marshal.WalEntryHeader{}, io.EOF
	}

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
	fd         *os.File
	RaftState  pb.HardState
	blocks     []byte
	syncPolicy *syncPolicyConfig
}

func openRaftStateSegment(fp string, policies ...*syncPolicyConfig) (rSeg *RaftStateSegment, err error) {
	fd := iooperator.OpenBufferIOFile(fp, os.O_CREATE|os.O_RDWR, 0644)

	rSeg = new(RaftStateSegment)
	rSeg.fd = fd
	rSeg.RaftState = pb.HardState{}
	rSeg.blocks = make([]byte, 0, hSSegmentHeaderSize+64)
	if len(policies) > 0 {
		rSeg.syncPolicy = policies[0]
	}
	fileInfo, _ := rSeg.fd.Stat()

	if fileInfo.Size() > 0 {
		data := make([]byte, int(fileInfo.Size()))
		readN, readErr := rSeg.fd.ReadAt(data, 0)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		rSeg.blocks = append(rSeg.blocks[:0], data[:readN]...)
		rSeg.decodeRaftStateSegment()
	}

	return rSeg, nil
}

func (seg *RaftStateSegment) Save(hs pb.HardState) (err error) {
	return seg.save(hs, true)
}

func (seg *RaftStateSegment) SaveNoSync(hs pb.HardState) (err error) {
	return seg.save(hs, false)
}

func (seg *RaftStateSegment) save(hs pb.HardState, doSync bool) (err error) {
	seg.RaftState = hs
	data, err := seg.encodeRaftStateSegment()
	if err != nil {
		return err
	}
	if err = writeStateBytes(seg.fd, data); err != nil {
		return err
	}
	seg.blocks = append(seg.blocks[:0], data...)
	if doSync && seg.syncPolicy != nil {
		if err = seg.syncPolicy.sync(seg.fd); err != nil {
			return err
		}
	}
	return nil
}

func (seg *RaftStateSegment) sync() error {
	if seg == nil || seg.fd == nil || seg.syncPolicy == nil {
		return nil
	}
	return seg.syncPolicy.sync(seg.fd)
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
	if len(seg.blocks) < hSSegmentHeaderSize {
		return
	}
	header := int(binary.LittleEndian.Uint32(seg.blocks[0:4]))
	if header <= 0 || header+hSSegmentHeaderSize > len(seg.blocks) {
		return
	}
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
	syncPolicy   *syncPolicyConfig
}

func OpenWalStateSegment(fp string, policies ...*syncPolicyConfig) (kvSeg *WalStateSegment, err error) {
	fd := iooperator.OpenBufferIOFile(fp, os.O_CREATE|os.O_RDWR, 0644)

	kvSeg = new(WalStateSegment)
	kvSeg.fd = fd
	kvSeg.blocks = make([]byte, 0, applyIndexSize+applyTermSize)
	if len(policies) > 0 {
		kvSeg.syncPolicy = policies[0]
	}
	fileInfo, _ := kvSeg.fd.Stat()

	if fileInfo.Size() > 0 {
		data := make([]byte, int(fileInfo.Size()))
		readN, readErr := kvSeg.fd.ReadAt(data, 0)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		kvSeg.blocks = append(kvSeg.blocks[:0], data[:readN]...)
		kvSeg.decodeWalStateSegment()
	}

	return kvSeg, nil
}

func (seg *WalStateSegment) Save(appliedIndex, appliedTerm uint64) (err error) {
	return seg.save(appliedIndex, appliedTerm, true)
}

func (seg *WalStateSegment) SaveNoSync(appliedIndex, appliedTerm uint64) (err error) {
	return seg.save(appliedIndex, appliedTerm, false)
}

func (seg *WalStateSegment) save(appliedIndex, appliedTerm uint64, doSync bool) (err error) {
	if appliedIndex != seg.AppliedIndex && appliedIndex > 0 {
		seg.AppliedIndex = appliedIndex
		seg.AppliedTerm = appliedTerm
	}

	data := seg.encodeWalStateSegment()
	if err = writeStateBytes(seg.fd, data); err != nil {
		return err
	}
	seg.blocks = append(seg.blocks[:0], data...)
	if doSync && seg.syncPolicy != nil {
		if err = seg.syncPolicy.sync(seg.fd); err != nil {
			return err
		}
	}
	return nil
}

func (seg *WalStateSegment) sync() error {
	if seg == nil || seg.fd == nil || seg.syncPolicy == nil {
		return nil
	}
	return seg.syncPolicy.sync(seg.fd)
}

func (seg *WalStateSegment) encodeWalStateSegment() []byte {
	buf := make([]byte, applyIndexSize+applyTermSize)
	binary.LittleEndian.PutUint64(buf[zero:applyIndexSize], seg.AppliedIndex)
	binary.LittleEndian.PutUint64(buf[applyIndexSize:applyIndexSize+applyTermSize], seg.AppliedTerm)
	return buf
}

func (seg *WalStateSegment) decodeWalStateSegment() {
	if len(seg.blocks) < applyIndexSize+applyTermSize {
		return
	}
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
	syncPolicy   *syncPolicyConfig
}

func OpenVlogStateSegment(fp string, policies ...*syncPolicyConfig) (kvSeg *VlogStateSegment, err error) {
	fd := iooperator.OpenBufferIOFile(fp, os.O_CREATE|os.O_RDWR, 0644)

	kvSeg = new(VlogStateSegment)
	kvSeg.fd = fd
	kvSeg.blocks = make([]byte, 0, persistIndexSize)
	if len(policies) > 0 {
		kvSeg.syncPolicy = policies[0]
	}
	fileInfo, _ := kvSeg.fd.Stat()

	if fileInfo.Size() > 0 {
		data := make([]byte, int(fileInfo.Size()))
		readN, readErr := kvSeg.fd.ReadAt(data, 0)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		kvSeg.blocks = append(kvSeg.blocks[:0], data[:readN]...)
		kvSeg.decodeVlogStateSegment()
	}

	return kvSeg, nil
}

func (seg *VlogStateSegment) Save(persistIndex uint64) (err error) {
	return seg.save(persistIndex, true)
}

func (seg *VlogStateSegment) SaveNoSync(persistIndex uint64) (err error) {
	return seg.save(persistIndex, false)
}

func (seg *VlogStateSegment) save(persistIndex uint64, doSync bool) (err error) {
	if persistIndex != seg.PersistIndex && persistIndex > 0 {
		seg.PersistIndex = persistIndex
	}

	data := seg.encodeVlogStateSegment()
	if err = writeStateBytes(seg.fd, data); err != nil {
		return err
	}
	seg.blocks = append(seg.blocks[:0], data...)
	if doSync && seg.syncPolicy != nil {
		if err = seg.syncPolicy.sync(seg.fd); err != nil {
			return err
		}
	}
	return nil
}

func (seg *VlogStateSegment) sync() error {
	if seg == nil || seg.fd == nil || seg.syncPolicy == nil {
		return nil
	}
	return seg.syncPolicy.sync(seg.fd)
}

func (seg *VlogStateSegment) encodeVlogStateSegment() []byte {
	buf := make([]byte, persistIndexSize)
	binary.LittleEndian.PutUint64(buf[zero:persistIndexSize], seg.PersistIndex)
	return buf
}

func (seg *VlogStateSegment) decodeVlogStateSegment() {
	if len(seg.blocks) < persistIndexSize {
		return
	}
	seg.PersistIndex = binary.LittleEndian.Uint64(seg.blocks[zero:persistIndexSize])
	return
}

func (seg *VlogStateSegment) Close() error {
	return seg.fd.Close()
}

func (seg *VlogStateSegment) Remove() error {
	return os.Remove(seg.fd.Name())
}

func writeStateBytes(fd *os.File, data []byte) error {
	if fd == nil {
		return os.ErrInvalid
	}
	n, err := fd.WriteAt(data, 0)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
