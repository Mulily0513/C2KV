package marshal

import (
	"encoding/binary"
	"github.com/Mulily0513/C2KV/pb"
	"hash/crc32"
)

const (
	// ChunkHeaderSize Checksum Length  index
	//    4       4       8
	ChunkHeaderSize = 16

	Crc32Size     = 4
	EntrySize     = 4
	IndexSize     = 8
	TypeDelete    = 1
	TypeInsert    = 2
	TimeStampSize = 8
	TypeSize      = 1
	ApplySigSize  = 36
	KeySize       = 4
)

type BytesKV struct {
	Key   []byte
	Value []byte
}

type KV struct {
	ApplySig []byte // uuid 36B
	KeySize  uint32
	Key      []byte
	Data     *Data //Data needs to be serialized as a whole and written into the skip list.
}

func EncodeKV(kv *KV) []byte {
	dataBytes := EncodeData(kv.Data) // Serialize Data struct
	keyLen := len(kv.Key)
	buf := make([]byte, ApplySigSize+KeySize+keyLen+len(dataBytes))
	copy(buf[:ApplySigSize], kv.ApplySig)
	binary.LittleEndian.PutUint32(buf[ApplySigSize:ApplySigSize+KeySize], uint32(keyLen))
	copy(buf[ApplySigSize+KeySize:], kv.Key)
	copy(buf[ApplySigSize+KeySize+keyLen:], dataBytes)
	return buf
}

func DecodeKV(data []byte) (kv *KV) {
	kv = &KV{}
	kv.ApplySig = data[:ApplySigSize]
	kv.KeySize = binary.LittleEndian.Uint32(data[ApplySigSize : ApplySigSize+KeySize])
	kv.Key = data[ApplySigSize+KeySize : ApplySigSize+KeySize+kv.KeySize]
	kv.Data = DecodeData(data[ApplySigSize+KeySize+kv.KeySize:])
	return kv
}

type Data struct {
	Index     uint64 //for raft log
	TimeStamp int64
	Type      int8 //insert or delete
	Value     []byte
}

func EncodeData(v *Data) []byte {
	buf := make([]byte, IndexSize+TimeStampSize+TypeSize+len(v.Value))
	binary.LittleEndian.PutUint64(buf[:IndexSize], v.Index)
	binary.LittleEndian.PutUint64(buf[IndexSize:IndexSize+TimeStampSize], uint64(v.TimeStamp))
	buf[IndexSize+TimeStampSize] = byte(v.Type)
	copy(buf[IndexSize+TimeStampSize+TypeSize:], v.Value)
	return buf
}

func DecodeData(v []byte) *Data {
	data := &Data{}
	data.Index = binary.LittleEndian.Uint64(v[:IndexSize])
	data.TimeStamp = int64(binary.LittleEndian.Uint64(v[IndexSize : IndexSize+TimeStampSize]))
	data.Type = int8(v[IndexSize+TimeStampSize])
	data.Value = v[IndexSize+TimeStampSize+TypeSize:]
	return data
}

// EncodeWALEntry  will encode entry into a byte slice.
// +-------+-----------+--------+-----------+
// |  crc  | entry size|  index |   entry
// +-------+-----------+--------+-----------+
// |----------HEADER------------|---BODY----+

type WalEntryHeader struct {
	Crc32     int
	EntrySize int
	Index     uint64
}

func (h WalEntryHeader) IsEmpty() bool {
	return h.Crc32 == 0 || h.EntrySize == 0 || h.Index == 0
}

func EncodeWALEntry(e *pb.Entry) ([]byte, int) {
	eBytes, _ := e.Marshal()
	eBytesSize := len(eBytes)
	var size = ChunkHeaderSize + eBytesSize
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[Crc32Size:], uint32(eBytesSize))
	binary.LittleEndian.PutUint64(buf[Crc32Size+EntrySize:], e.Index)
	copy(buf[Crc32Size+EntrySize+IndexSize:], eBytes)

	crc := crc32.ChecksumIEEE(buf[Crc32Size+EntrySize+IndexSize:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func DecodeWALEntryHeader(buf []byte) (header WalEntryHeader) {
	crc32 := binary.LittleEndian.Uint32(buf[:Crc32Size])
	entrySize := binary.LittleEndian.Uint16(buf[Crc32Size : Crc32Size+EntrySize])
	index := binary.LittleEndian.Uint64(buf[Crc32Size+EntrySize : Crc32Size+EntrySize+IndexSize])
	header.Crc32 = int(crc32)
	header.EntrySize = int(entrySize)
	header.Index = index
	return
}

const (
	FidOffset         = 0
	ValueOffset       = 8
	ValueSize         = 16
	TimeStamp         = 24
	ValueCrc32        = 28
	SerializationSize = 36
)

type IndexerMeta struct {
	SSTid       uint64
	ValueOffset int64
	ValueSize   int64
	TimeStamp   int64
	ValueCrc32  uint32
	Value       []byte //smaller value could be place in index
}

func EncodeIndexMeta(m *IndexerMeta) []byte {
	valueSize := len(m.Value)
	buf := make([]byte, SerializationSize+valueSize)
	binary.LittleEndian.PutUint64(buf[FidOffset:ValueOffset], m.SSTid)
	binary.LittleEndian.PutUint64(buf[ValueOffset:ValueSize], uint64(m.ValueOffset))
	binary.LittleEndian.PutUint64(buf[ValueSize:TimeStamp], uint64(m.ValueSize))
	binary.LittleEndian.PutUint32(buf[TimeStamp:ValueCrc32], m.ValueCrc32)
	binary.LittleEndian.PutUint64(buf[ValueCrc32:SerializationSize], uint64(m.TimeStamp))
	copy(buf[SerializationSize:], m.Value)
	return buf
}

func DecodeIndexMeta(buf []byte) *IndexerMeta {
	m := &IndexerMeta{}
	m.SSTid = binary.LittleEndian.Uint64(buf[FidOffset:ValueOffset])
	m.ValueOffset = int64(int(binary.LittleEndian.Uint64(buf[ValueOffset:ValueSize])))
	m.ValueSize = int64(int(binary.LittleEndian.Uint64(buf[ValueSize:TimeStamp])))
	m.ValueCrc32 = binary.LittleEndian.Uint32(buf[TimeStamp:ValueCrc32])
	m.TimeStamp = int64(binary.LittleEndian.Uint64(buf[ValueCrc32:SerializationSize]))
	m.Value = make([]byte, len(buf)-SerializationSize)
	copy(m.Value, buf[SerializationSize:])
	return m
}
