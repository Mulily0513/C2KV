package wal

import (
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db/iooperator"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/Mulily0513/C2KV/internal/telemetry"
	"os"
	"sync"
	"time"
)

type syncMode int

const (
	syncModeAlways syncMode = iota + 1
	syncModeBatch
	syncModeNone
)

type syncPolicyConfig struct {
	mode       syncMode
	interval   time.Duration
	lastSynced time.Time
	syncing    bool
	mu         sync.Mutex
}

func newSyncPolicy(cfg config.WalConfig) *syncPolicyConfig {
	mode := syncModeAlways
	switch cfg.NormalizeSyncMode() {
	case config.WALSyncBatch:
		mode = syncModeBatch
	case config.WALSyncNone:
		mode = syncModeNone
	}
	return &syncPolicyConfig{
		mode:     mode,
		interval: time.Duration(cfg.NormalizedSyncIntervalMs()) * time.Millisecond,
	}
}

func (p *syncPolicyConfig) sync(fd *os.File) error {
	if fd == nil {
		return nil
	}
	switch p.mode {
	case syncModeNone:
		return nil
	case syncModeAlways:
		return syncAndObserve(fd)
	case syncModeBatch:
		p.triggerBatchSync(fd)
		return nil
	default:
		return syncAndObserve(fd)
	}
}

func (p *syncPolicyConfig) triggerBatchSync(fd *os.File) {
	p.mu.Lock()
	now := time.Now()
	if p.syncing {
		p.mu.Unlock()
		return
	}
	if !p.lastSynced.IsZero() && now.Sub(p.lastSynced) < p.interval {
		p.mu.Unlock()
		return
	}
	p.syncing = true
	p.mu.Unlock()

	go func() {
		err := syncAndObserve(fd)
		now := time.Now()
		p.mu.Lock()
		if err == nil {
			p.lastSynced = now
		}
		p.syncing = false
		p.mu.Unlock()
		if err != nil {
			log.Warnf("batch wal sync failed: %v", err)
		}
	}()
}

func syncAndObserve(fd *os.File) error {
	start := time.Now()
	err := iooperator.FdatasyncFile(fd)
	telemetry.ObserveWALSync(time.Since(start))
	return err
}
