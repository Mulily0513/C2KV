package config

type DBConfig struct {
	DBPath string

	MemConfig MemConfig

	ValueLogConfig ValueLogConfig

	WalConfig WalConfig
}

type MemConfig struct {
	MemTableNums int

	MemTablePipeSize int

	// Default value is 64MB. MB Unit
	MemTableSize int64

	// memTable的写入并发度
	Concurrency int
}

type WalConfig struct {
	WalDirPath string

	SegmentSize int //specifies the maximum size of each segment file in bytes. SegmentSize int64
}

type ValueLogConfig struct {
	ValueLogDir string

	PartitionNums int

	SSTSize int
}
