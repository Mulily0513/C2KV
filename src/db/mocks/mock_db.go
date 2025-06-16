package mocks

import (
	"github.com/Mulily0513/C2KV/src/config"
	"os"
	"path"
)

var wd, _ = os.Getwd()
var TestDBPath = path.Join(wd, "C2KV")
var TestDBConfig = config.DBConfig{TestDBPath, TestMemConfig, TestVlogCfg, TestWALCfg64Size}
