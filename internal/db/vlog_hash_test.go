package db

import (
	"testing"

	"github.com/Mulily0513/C2KV/internal/config"
)

func TestNewPartitionHasherDefault(t *testing.T) {
	hasher := newPartitionHasher(config.ValueLogConfig{})
	if hasher == nil {
		t.Fatal("expected default hasher")
	}
	key := []byte("alpha")
	if hasher(key) != hasher(key) {
		t.Fatal("default hasher must be deterministic")
	}
}

func TestNewPartitionHasherFNV64a(t *testing.T) {
	hasher := newPartitionHasher(config.ValueLogConfig{
		PartitionHash: config.PartitionHashFNV64a,
	})
	if hasher == nil {
		t.Fatal("expected fnv64a hasher")
	}
	keyA := []byte("alpha")
	keyB := []byte("beta")
	hashA1 := hasher(keyA)
	hashA2 := hasher(keyA)
	hashB := hasher(keyB)
	if hashA1 != hashA2 {
		t.Fatal("fnv64a hasher must be deterministic")
	}
	if hashA1 == hashB {
		t.Fatal("fnv64a hasher should distinguish common keys")
	}
}

func TestValueLogGetKeyPartitionRange(t *testing.T) {
	v := &ValueLog{
		vlogCfg: config.ValueLogConfig{
			PartitionNums: 8,
			PartitionHash: config.PartitionHashFNV64a,
		},
	}
	v.partitionHasher = newPartitionHasher(v.vlogCfg)

	for _, key := range [][]byte{
		[]byte("key-1"),
		[]byte("key-2"),
		[]byte("key-3"),
		[]byte("key-4"),
	} {
		p := v.getKeyPartition(key)
		if p >= uint64(v.vlogCfg.PartitionNums) {
			t.Fatalf("partition out of range: %d", p)
		}
	}
}
