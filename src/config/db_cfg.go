package config

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
}

type ValueLogConfig struct {
	ValueLogDir string `yaml:"valueLogDir"  json:"valueLogDir"`

	PartitionNums int `yaml:"partitionNums" json:"partitionNums"`

	SSTSize int `yaml:"sstSize" json:"sstSize"`
}
