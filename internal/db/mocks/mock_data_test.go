package mocks

import (
	"fmt"
	"testing"
)

func TestCreateKvsSize(t *testing.T) {
	nums := 100
	length := 250
	kvs := CreateKVs(nums, length, true)
	fmt.Printf(CreatKVsFmt, nums, length, ConvertSize(marshalKVs(kvs)))
}

func TestCreateEntriesSize(t *testing.T) {
	_, bytesCount := MarshalWALEntries(CreateEntries(10, 250))
	fmt.Printf(CreatEntriesFmt, 1, 10, ConvertSize(bytesCount))
}

func ConvertSize(size int) string {
	units := []string{"B", "KB", "MB", "GB"}
	if size == 0 {
		return "0" + units[0]
	}
	i := 0
	for size >= 1024 {
		size /= 1024
		i++
	}
	return fmt.Sprintf("%.f", float64(size)) + units[i]
}
