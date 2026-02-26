package wal

import (
	"os"
	"testing"
	"time"
)

func newTempFile(t *testing.T) *os.File {
	t.Helper()
	fd, err := os.CreateTemp(t.TempDir(), "sync-policy-*")
	if err != nil {
		t.Fatalf("create temp file failed: %v", err)
	}
	return fd
}

func newClosedTempFile(t *testing.T) *os.File {
	t.Helper()
	fd := newTempFile(t)
	if err := fd.Close(); err != nil {
		t.Fatalf("close temp file failed: %v", err)
	}
	return fd
}

func TestSyncPolicyNoneSkipsSyncErrors(t *testing.T) {
	fd := newClosedTempFile(t)
	policy := &syncPolicyConfig{mode: syncModeNone}

	if err := policy.sync(fd); err != nil {
		t.Fatalf("syncModeNone should skip sync errors, got: %v", err)
	}
}

func TestSyncPolicyAlwaysReturnsSyncError(t *testing.T) {
	fd := newClosedTempFile(t)
	policy := &syncPolicyConfig{mode: syncModeAlways}

	if err := policy.sync(fd); err == nil {
		t.Fatal("syncModeAlways should return sync error on closed fd")
	}
}

func TestSyncPolicyBatchSkipsSyncWithinInterval(t *testing.T) {
	fd := newClosedTempFile(t)
	last := time.Now()
	policy := &syncPolicyConfig{
		mode:       syncModeBatch,
		interval:   time.Hour,
		lastSynced: last,
	}

	if err := policy.sync(fd); err != nil {
		t.Fatalf("batch sync should be skipped within interval: %v", err)
	}
	if !policy.lastSynced.Equal(last) {
		t.Fatalf("lastSynced should not move when sync is skipped, before=%v after=%v", last, policy.lastSynced)
	}
}

func TestSyncPolicyBatchAttemptsSyncAfterInterval(t *testing.T) {
	fd := newClosedTempFile(t)
	last := time.Now().Add(-2 * time.Hour)
	policy := &syncPolicyConfig{
		mode:       syncModeBatch,
		interval:   time.Hour,
		lastSynced: last,
	}

	if err := policy.sync(fd); err != nil {
		t.Fatalf("batch sync should return quickly without blocking error, got: %v", err)
	}
	waitForSyncIdle(t, policy)
	if policy.syncing {
		t.Fatal("batch sync should eventually finish background sync")
	}
	if !policy.lastSynced.Equal(last) {
		t.Fatalf("lastSynced should not move on failed sync, before=%v after=%v", last, policy.lastSynced)
	}
}

func TestSyncPolicyBatchUpdatesLastSyncedOnSuccess(t *testing.T) {
	fd := newTempFile(t)
	defer func() { _ = fd.Close() }()

	last := time.Now().Add(-time.Second)
	policy := &syncPolicyConfig{
		mode:       syncModeBatch,
		interval:   10 * time.Millisecond,
		lastSynced: last,
	}

	if err := policy.sync(fd); err != nil {
		t.Fatalf("batch sync failed: %v", err)
	}
	waitForSyncIdle(t, policy)
	if !policy.lastSynced.After(last) {
		t.Fatalf("lastSynced should move forward on successful sync, before=%v after=%v", last, policy.lastSynced)
	}
}

func waitForSyncIdle(t *testing.T, policy *syncPolicyConfig) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		policy.mu.Lock()
		syncing := policy.syncing
		policy.mu.Unlock()
		if !syncing {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("batch sync did not become idle in time")
}
