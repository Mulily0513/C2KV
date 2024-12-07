package mocks

import (
	"fmt"
	"github.com/Mulily0513/C2KV/config"
	"os"
	"path"
)

const PartitionFormat = "PARTITION_%d"

var TestVlogCfg = config.ValueLogConfig{ValueLogDir: ValueLogPath, PartitionNums: 3}
var ValueLogPath = path.Join(TestDBPath, "VLOG")
var PartitionDir1 = path.Join(CurDirPath, fmt.Sprintf(PartitionFormat, 1))
var PartitionDir2 = path.Join(CurDirPath, fmt.Sprintf(PartitionFormat, 2))
var PartitionDir3 = path.Join(CurDirPath, fmt.Sprintf(PartitionFormat, 3))

func CreateValueLogDirIfNotExist(vlogDir string) {
	if _, err := os.Stat(vlogDir); err != nil {
		if err := os.Mkdir(vlogDir, 0755); err != nil {
			panic(err)
		}
	}
}
