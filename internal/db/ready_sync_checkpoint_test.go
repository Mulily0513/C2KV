package db

import "testing"

func openTestDBWithWalStateCheckpoint(t *testing.T, checkpoint int) *C2KV {
	t.Helper()
	cfg := newIsolatedDBConfig(t)
	cfg.WalConfig.WalStateCheckpoint = checkpoint
	db := OpenKVStorage(&cfg)
	t.Cleanup(db.Close)
	return db
}

func TestFinishReadySyncWalStateDefaultSyncEveryReady(t *testing.T) {
	db := openTestDBWithWalStateCheckpoint(t, 0)

	db.BeginReadySync(true)
	db.markReadyWalStateDirty()
	if err := db.FinishReadySync(); err != nil {
		t.Fatalf("finish ready sync failed: %v", err)
	}
	if db.walStatePendingSyncs != 0 {
		t.Fatalf("default checkpoint should sync every ready, pending=%d", db.walStatePendingSyncs)
	}
}

func TestFinishReadySyncWalStateCheckpointBatching(t *testing.T) {
	db := openTestDBWithWalStateCheckpoint(t, 2)

	db.BeginReadySync(true)
	db.markReadyWalStateDirty()
	if err := db.FinishReadySync(); err != nil {
		t.Fatalf("first finish ready sync failed: %v", err)
	}
	if db.walStatePendingSyncs != 1 {
		t.Fatalf("first dirty ready should defer wal-state sync, pending=%d", db.walStatePendingSyncs)
	}

	db.BeginReadySync(true)
	db.markReadyWalStateDirty()
	if err := db.FinishReadySync(); err != nil {
		t.Fatalf("second finish ready sync failed: %v", err)
	}
	if db.walStatePendingSyncs != 0 {
		t.Fatalf("checkpoint threshold should trigger sync and reset pending, pending=%d", db.walStatePendingSyncs)
	}
}

func TestFinishReadySyncWalStateNoMustSyncDoesNotCount(t *testing.T) {
	db := openTestDBWithWalStateCheckpoint(t, 2)

	db.BeginReadySync(false)
	db.markReadyWalStateDirty()
	if err := db.FinishReadySync(); err != nil {
		t.Fatalf("finish ready sync failed: %v", err)
	}
	if db.walStatePendingSyncs != 0 {
		t.Fatalf("non-mustSync ready should not advance checkpoint, pending=%d", db.walStatePendingSyncs)
	}
}
