package marshal

import (
	"github.com/Mulily0513/C2KV/pb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestEncodeANdDecodeWALEntry(t *testing.T) {
	entry1 := &pb.Entry{
		Term:  1,
		Index: 1,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	}

	wEntBytes, _ := EncodeWALEntry(entry1)
	buf := make([]byte, ChunkHeaderSize)
	copy(buf, wEntBytes[:ChunkHeaderSize])
	header := DecodeWALEntryHeader(buf)
	entry2 := &pb.Entry{}

	entry2.Unmarshal(wEntBytes[ChunkHeaderSize : ChunkHeaderSize+header.EntrySize])
	assert.EqualValues(t, entry1, entry2)
}

func TestIndexMetaEncodeDecode(t *testing.T) {
	m := &IndexerMeta{
		SSTid:       123,
		ValueOffset: 45623232323,
		ValueSize:   789232323,
		TimeStamp:   1000000,
		ValueCrc32:  987654,
		Value:       []byte("test value 1123232"),
	}

	serialized := EncodeIndexMeta(m)
	deserialized := DecodeIndexMeta(serialized)

	if !reflect.DeepEqual(m, deserialized) {
		t.Errorf("Serialization and deserialization do not match")
	}
}

func TestEncodeDecodeData(t *testing.T) {
	v := &Data{
		Index:     123,
		TimeStamp: 456,
		Type:      1,
		Value:     []byte("test"),
	}
	encodedData := EncodeData(v)
	decodedData := DecodeData(encodedData)
	assert.EqualValues(t, v, decodedData)
}

func TestEncodeDecodeKV(t *testing.T) {
	kv := &KV{
		ApplySig: []byte(uuid.New().String()),
		KeySize:  uint32(len([]byte("key"))),
		Key:      []byte("key"),
		Data: &Data{
			Index:     123,
			TimeStamp: 456,
			Type:      1,
			Value:     []byte("test"),
		},
	}
	encodedKV := EncodeKV(kv)
	decodedKV := DecodeKV(encodedKV)

	assert.EqualValues(t, kv, decodedKV)
}
