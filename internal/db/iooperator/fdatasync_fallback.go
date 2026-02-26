//go:build !linux

package iooperator

import "os"

func FdatasyncFile(f *os.File) error {
	if f == nil {
		return nil
	}
	return f.Sync()
}
