package mocks

import (
	"os"
	"path"
)

var ValueLogPath = path.Join(DBPath, "VLOG")

func CreateValueLogDirIfNotExist(vlogDir string) {
	if _, err := os.Stat(vlogDir); err != nil {
		if err := os.Mkdir(vlogDir, 0755); err != nil {
			panic(err)
		}
	}
}
