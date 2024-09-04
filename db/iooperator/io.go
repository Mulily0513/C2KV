package iooperator

import (
	"github.com/Mulily0513/C2KV/db/iooperator/directio"
	"github.com/Mulily0513/C2KV/log"
	"os"
)

func OpenDirectIOFile(fp string, flag int, perm os.FileMode) (file *os.File, err error) {
	return directio.OpenDirectFile(fp, flag, perm)
}

func OpenBufferIOFile(fp string, flag int, perm os.FileMode) *os.File {
	file, err := os.OpenFile(fp, flag, perm)
	if err != nil {
		log.Panicf("open buffer io file failed %v", err)
	}
	return file
}
