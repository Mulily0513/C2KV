package mocks

import (
	"fmt"
	"testing"
)

func TestCreateKvs(t *testing.T) {
	nums := 100
	length := 250
	kvs := CreateKVs(nums, length, true)
	fmt.Printf(CreatKVsFmt, nums, length, ConvertSize(marshalKVs(kvs)))
}
