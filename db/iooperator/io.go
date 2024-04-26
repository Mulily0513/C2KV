package iooperator

import (
	"github.com/Mulily0513/C2KV/db/iooperator/directio"
	"github.com/Mulily0513/C2KV/log"
	"os"
)

func OpenDirectIOFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return directio.OpenDirectFile(name, flag, perm)
}

func OpenBufferIOFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Panicf("", err)
	}
	return file
}
