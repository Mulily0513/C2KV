package iooperator

import (
	"github.com/Mulily0513/C2KV/db/iooperator/directio"
	"github.com/Mulily0513/C2KV/log"
	"os"
)

func OpenDirectIOFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return directio.OpenDirectFile(name, flag, perm)
}

func OpenBufferIOFile(name string, flag int, perm os.FileMode) *os.File {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		log.Panicf("open buffer io file failed", err)
	}
	return file
}
