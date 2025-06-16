package db

import (
	"github.com/Mulily0513/C2KV/src/db/mocks"
	"github.com/Mulily0513/C2KV/src/pb"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

func TestC2KV_OpenKVStorage(t *testing.T) {
	OpenKVStorage(&mocks.TestDBConfig)
}

func TestC2KV_PersistHardStateAndInitialState(t *testing.T) {
	tHardState := pb.HardState{Term: 1, Vote: 2, Commit: 3}
	kt := OpenKVStorage(&mocks.TestDBConfig)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go kt.PersistHardState(tHardState, wg)
	wg.Wait()
	if kt.InitialState() != tHardState {
		t.Error("PersistHardState failed")
	}
}

func TestC2KV_PersistUnstableEntsAndEntries(t *testing.T) {
	kt := OpenKVStorage(&mocks.TestDBConfig)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go kt.PersistUnstableEnts(mocks.Entries_20_250_3KB, wg)
	wg.Wait()
	ents, err := kt.Entries(mocks.Entries_20_250_3KB[0].Index, mocks.Entries_20_250_3KB[len(mocks.Entries_20_250_3KB)-1].Index)
	if err != nil {
		t.Error("PersistUnstableEnts failed")
	}
	reflect.DeepEqual(mocks.Entries_20_250_3KB, ents)
	kt.Remove()
}

func TestC2KV_Apply(t *testing.T) {
	nums := 500
	kt := OpenKVStorage(&mocks.TestDBConfig)
	ents, kvs := mocks.MockApplyData(nums)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	kt.PersistUnstableEnts(ents, wg)
	wg.Wait()
	if err := kt.Apply(kvs[:nums/2]); err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, kt.FirstIndex(), 251)
	assert.EqualValues(t, kt.StableIndex(), 500)
	assert.EqualValues(t, kt.wal.WalStateSegment.AppliedIndex, 250)
	assert.EqualValues(t, kt.wal.WalStateSegment.AppliedTerm, 250)
	kt.Remove()
}
