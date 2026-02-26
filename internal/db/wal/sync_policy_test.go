package wal

import (
	"github.com/Mulily0513/C2KV/internal/config"
	"testing"
	"time"
)

func TestNewSyncPolicyMode(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.WalConfig
		want syncMode
	}{
		{
			name: "default always",
			cfg:  config.WalConfig{},
			want: syncModeAlways,
		},
		{
			name: "batch",
			cfg: config.WalConfig{
				SyncMode:       config.WALSyncBatch,
				SyncIntervalMs: 10,
			},
			want: syncModeBatch,
		},
		{
			name: "none",
			cfg: config.WalConfig{
				SyncMode: config.WALSyncNone,
			},
			want: syncModeNone,
		},
		{
			name: "invalid fallback always",
			cfg: config.WalConfig{
				SyncMode: "invalid",
			},
			want: syncModeAlways,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newSyncPolicy(tt.cfg)
			if p.mode != tt.want {
				t.Fatalf("mode mismatch, want=%v got=%v", tt.want, p.mode)
			}
		})
	}
}

func TestNewSyncPolicyBatchIntervalDefault(t *testing.T) {
	p := newSyncPolicy(config.WalConfig{SyncMode: config.WALSyncBatch})
	if p.interval != 100*time.Millisecond {
		t.Fatalf("unexpected interval, want=100ms got=%v", p.interval)
	}
}
