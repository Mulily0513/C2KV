package db

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func newIsolatedDBConfig(t *testing.T) config.DBConfig {
	t.Helper()
	cfg := mocks.TestDBConfig
	root := t.TempDir()
	cfg.DBPath = root
	cfg.WalConfig.WalDirPath = filepath.Join(root, "wal")
	cfg.ValueLogConfig.ValueLogDir = filepath.Join(root, "vlog")
	return cfg
}

func openTestDB(t *testing.T) *C2KV {
	t.Helper()
	cfg := newIsolatedDBConfig(t)
	db := OpenKVStorage(&cfg)
	t.Cleanup(db.Close)
	return db
}

func TestC2KV_OpenKVStorage(t *testing.T) {
	_ = openTestDB(t)
}

func TestC2KV_PersistHardStateAndInitialState(t *testing.T) {
	tHardState := pb.HardState{Term: 1, Vote: 2, Commit: 3}
	kt := openTestDB(t)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go kt.PersistHardState(tHardState, wg)
	wg.Wait()
	if kt.InitialState() != tHardState {
		t.Error("PersistHardState failed")
	}
}

func TestC2KV_PersistUnstableEntsAndEntries(t *testing.T) {
	kt := openTestDB(t)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go kt.PersistUnstableEnts(mocks.Entries_20_250_3KB, wg)
	wg.Wait()
	ents, err := kt.Entries(mocks.Entries_20_250_3KB[0].Index, mocks.Entries_20_250_3KB[len(mocks.Entries_20_250_3KB)-1].Index)
	if err != nil {
		t.Error("PersistUnstableEnts failed")
	}
	reflect.DeepEqual(mocks.Entries_20_250_3KB, ents)
}

func TestC2KV_Apply(t *testing.T) {
	nums := 500
	kt := openTestDB(t)
	ents, kvs := mocks.MockApplyData(nums)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	kt.PersistUnstableEnts(ents, wg)
	wg.Wait()
	if err := kt.Apply(kvs[:nums/2]); err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, kt.FirstIndex(), 1)
	assert.EqualValues(t, kt.StableIndex(), 500)
	assert.EqualValues(t, kt.wal.WalStateSegment.AppliedIndex, 250)
	assert.EqualValues(t, kt.wal.WalStateSegment.AppliedTerm, 250)
}
