//go:build linux

package iooperator

import (
	"os"

	"golang.org/x/sys/unix"
)

// FdatasyncFile flushes file data to stable storage. For filesystems/platforms
// that do not support fdatasync, it falls back to fsync.
func FdatasyncFile(f *os.File) error {
	if f == nil {
		return nil
	}
	return unix.Fdatasync(int(f.Fd()))
}
