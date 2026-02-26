package config

import "testing"

func TestWalConfigNormalizeSyncMode(t *testing.T) {
	tests := []struct {
		name string
		cfg  WalConfig
		want string
	}{
		{
			name: "default fallback",
			cfg:  WalConfig{},
			want: WALSyncAlways,
		},
		{
			name: "trim and lower",
			cfg:  WalConfig{SyncMode: "  BATCH  "},
			want: WALSyncBatch,
		},
		{
			name: "none",
			cfg:  WalConfig{SyncMode: "none"},
			want: WALSyncNone,
		},
		{
			name: "invalid fallback",
			cfg:  WalConfig{SyncMode: "x"},
			want: WALSyncAlways,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.NormalizeSyncMode()
			if got != tt.want {
				t.Fatalf("NormalizeSyncMode() mismatch, got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestWalConfigNormalizedSyncIntervalMs(t *testing.T) {
	tests := []struct {
		name string
		cfg  WalConfig
		want int
	}{
		{
			name: "default",
			cfg:  WalConfig{},
			want: 100,
		},
		{
			name: "negative fallback",
			cfg:  WalConfig{SyncIntervalMs: -1},
			want: 100,
		},
		{
			name: "use configured interval",
			cfg:  WalConfig{SyncIntervalMs: 250},
			want: 250,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.NormalizedSyncIntervalMs()
			if got != tt.want {
				t.Fatalf("NormalizedSyncIntervalMs() mismatch, got=%d want=%d", got, tt.want)
			}
		})
	}
}

func TestWalConfigNormalizedWalStateCheckpoint(t *testing.T) {
	tests := []struct {
		name string
		cfg  WalConfig
		want int
	}{
		{
			name: "default",
			cfg:  WalConfig{},
			want: 1,
		},
		{
			name: "negative fallback",
			cfg:  WalConfig{WalStateCheckpoint: -2},
			want: 1,
		},
		{
			name: "use configured checkpoint",
			cfg:  WalConfig{WalStateCheckpoint: 32},
			want: 32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.NormalizedWalStateCheckpoint()
			if got != tt.want {
				t.Fatalf("NormalizedWalStateCheckpoint() mismatch, got=%d want=%d", got, tt.want)
			}
		})
	}
}

func TestValueLogConfigNormalizePartitionHash(t *testing.T) {
	tests := []struct {
		name string
		cfg  ValueLogConfig
		want string
	}{
		{
			name: "default fallback",
			cfg:  ValueLogConfig{},
			want: PartitionHashSHA256,
		},
		{
			name: "trim and lower",
			cfg:  ValueLogConfig{PartitionHash: "  FNV64A  "},
			want: PartitionHashFNV64a,
		},
		{
			name: "sha256 explicit",
			cfg:  ValueLogConfig{PartitionHash: "sha256"},
			want: PartitionHashSHA256,
		},
		{
			name: "invalid fallback",
			cfg:  ValueLogConfig{PartitionHash: "md5"},
			want: PartitionHashSHA256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.NormalizePartitionHash()
			if got != tt.want {
				t.Fatalf("NormalizePartitionHash() mismatch, got=%q want=%q", got, tt.want)
			}
		})
	}
}
