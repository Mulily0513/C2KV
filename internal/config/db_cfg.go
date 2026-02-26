package config

import "strings"

type DBConfig struct {
	DBPath string `yaml:"dbPath" json:"dbPath"`

	MemConfig MemConfig `yaml:"memConfig"  json:"memConfig"`

	ValueLogConfig ValueLogConfig `yaml:"valueLogConfig" json:"valueLogConfig"`

	WalConfig WalConfig `yaml:"walConfig" json:"walConfig"`
}

type MemConfig struct {
	MemTableNums int `yaml:"memTableNums"  json:"memTableNums"`

	MemTablePipeSize int `yaml:"memTablePipeSize"  json:"memTablePipeSize"`

	// Default value is 64MB. MB Unit
	MemTableSize int64 `yaml:"memTableSize" json:"memTableSize"`

	// Write concurrent threads of memTable
	Concurrency int `yaml:"concurrency"  json:"concurrency"`
}

type WalConfig struct {
	WalDirPath string `yaml:"walDirPath" json:"walDirPath"`

	//specifies the maximum size of each segment file in bytes, MB unit
	SegmentSize int `yaml:"segmentSize"  json:"segmentSize"`

	// SyncMode controls WAL fsync policy: always | batch | none.
	SyncMode string `yaml:"syncMode" json:"syncMode"`
	// SyncIntervalMs is used only when SyncMode=batch.
	SyncIntervalMs int `yaml:"syncIntervalMs" json:"syncIntervalMs"`
	// WalStateCheckpoint controls how many applied-index updates can be
	// accumulated before syncing WAL state segment. 1 means sync on every ready.
	WalStateCheckpoint int `yaml:"walStateCheckpoint" json:"walStateCheckpoint"`
}

type ValueLogConfig struct {
	ValueLogDir string `yaml:"valueLogDir"  json:"valueLogDir"`

	PartitionNums int `yaml:"partitionNums" json:"partitionNums"`

	SSTSize int `yaml:"sstSize" json:"sstSize"`

	// PartitionHash controls key->partition hashing strategy.
	// Supported: sha256 | fnv64a.
	// Default: sha256 (compatibility-first).
	PartitionHash string `yaml:"partitionHash" json:"partitionHash"`
}

const (
	WALSyncAlways = "always"
	WALSyncBatch  = "batch"
	WALSyncNone   = "none"

	PartitionHashSHA256 = "sha256"
	PartitionHashFNV64a = "fnv64a"
)

func (c WalConfig) NormalizeSyncMode() string {
	mode := strings.ToLower(strings.TrimSpace(c.SyncMode))
	switch mode {
	case WALSyncAlways, WALSyncBatch, WALSyncNone:
		return mode
	default:
		return WALSyncAlways
	}
}

func (c WalConfig) NormalizedSyncIntervalMs() int {
	if c.SyncIntervalMs <= 0 {
		return 100
	}
	return c.SyncIntervalMs
}

func (c WalConfig) NormalizedWalStateCheckpoint() int {
	if c.WalStateCheckpoint <= 0 {
		return 1
	}
	return c.WalStateCheckpoint
}

func (c ValueLogConfig) NormalizePartitionHash() string {
	hash := strings.ToLower(strings.TrimSpace(c.PartitionHash))
	switch hash {
	case PartitionHashSHA256, PartitionHashFNV64a:
		return hash
	default:
		return PartitionHashSHA256
	}
}
