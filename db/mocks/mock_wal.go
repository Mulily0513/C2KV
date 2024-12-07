package mocks

import (
	"github.com/Mulily0513/C2KV/config"
	"os"
)

var _64MB = 64
var _1MB = 1

var walWd, _ = os.Getwd()

var TestWALCfg64Size = config.WalConfig{
	WalDirPath:  walWd,
	SegmentSize: 64,
}

var TestWALCfg1Size = config.WalConfig{
	WalDirPath:  walWd,
	SegmentSize: 1,
}
