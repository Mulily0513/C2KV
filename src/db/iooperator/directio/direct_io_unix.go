// Direct IO for Unix

//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

package directio

import (
	"os"
	"syscall"
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT
func OpenDirectFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, syscall.O_DIRECT|flag, perm)
}
