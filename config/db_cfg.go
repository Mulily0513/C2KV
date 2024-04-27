package config

type DBConfig struct {
	DBPath string `yaml:"dbPath" json:"dbPath"`

	MemConfig MemConfig `yaml:"memConfig"  json:"memConfig"`

	ValueLogConfig ValueLogConfig `yaml:"valueLogConfig" json:"valueLogConfig"`

	WalConfig WalConfig `yaml:"walConfig"`
}

type MemConfig struct {
	MemTableNums int `yaml:"memTableNums"`

	MemTablePipeSize int `yaml:"memTablePipeSize"`

	// Default value is 64MB. MB Unit
	MemTableSize int64 `yaml:"memTableSize"`

	// memTable的写入并发度
	Concurrency int `yaml:"concurrency"`
}

type WalConfig struct {
	WalDirPath string `yaml:"walDirPath"`

	//specifies the maximum size of each segment file in bytes. SegmentSize int64
	SegmentSize int `yaml:"segmentSize"`
}

type ValueLogConfig struct {
	ValueLogDir string `yaml:"valueLogDir"`

	PartitionNums int `yaml:"partitionNums"`

	SSTSize int `yaml:"sstSize"`
}
