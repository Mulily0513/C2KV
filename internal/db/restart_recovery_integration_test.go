package db

import (
	"bytes"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"path/filepath"
	"sync"
	"testing"
)

func testDBConfig(base string) *config.DBConfig {
	dbPath := filepath.Join(base, "db")
	return &config.DBConfig{
		DBPath: dbPath,
		MemConfig: config.MemConfig{
			MemTableNums:     8,
			MemTablePipeSize: 8,
			MemTableSize:     64,
			Concurrency:      1,
		},
		ValueLogConfig: config.ValueLogConfig{
			ValueLogDir:   filepath.Join(dbPath, "vlog"),
			PartitionNums: 2,
			SSTSize:       64,
		},
		WalConfig: config.WalConfig{
			WalDirPath:  filepath.Join(dbPath, "wal"),
			SegmentSize: 1,
		},
	}
}

func TestOpenKVStorageRestartRecovery(t *testing.T) {
	cfg := testDBConfig(t.TempDir())
	db1 := OpenKVStorage(cfg)

	ents, kvs := mocks.MockApplyData(6)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go db1.PersistUnstableEnts(ents, wg)
	wg.Wait()

	wg.Add(1)
	go db1.PersistHardState(pb.HardState{Term: 1, Vote: 1, Commit: 5}, wg)
	wg.Wait()

	if err := db1.Apply(kvs[:4]); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	db1.Close()

	db2 := OpenKVStorage(cfg)
	t.Cleanup(func() {
		db2.Close()
		db2.Remove()
	})

	if db2.FirstIndex() != 4 || db2.StableIndex() != 5 {
		t.Fatalf("unexpected recovered entries range, first=%d last=%d", db2.FirstIndex(), db2.StableIndex())
	}

	kv, err := db2.Get(kvs[1].Key)
	if err != nil {
		t.Fatalf("failed to read restored key %q", string(kvs[1].Key))
	}
	if !bytes.Equal(kv.Data.Value, kvs[1].Data.Value) {
		t.Fatalf("unexpected restored value, got=%q want=%q", string(kv.Data.Value), string(kvs[1].Data.Value))
	}
}
