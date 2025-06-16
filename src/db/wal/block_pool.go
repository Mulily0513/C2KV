package wal

import (
	"github.com/Mulily0513/C2KV/src/log"
	"unsafe"
)

const (
	alignSize = 4096 //Both SSDs and HHDs are aligned at 4K by default
	block4096 = alignSize
	num4      = 4
	num8      = 8
	block4    = block4096 * 4
	block8    = block4096 * 8
)

type blockPool struct {
	block4 []byte //4*4096   16KB
	block8 []byte //8*4096   32KB
}

func newBlockPool() (bp *blockPool) {
	bp = new(blockPool)
	bp.block4 = alignedCustomBlock(num4)
	bp.block8 = alignedCustomBlock(num8)
	return bp
}

// AlignedBlock  alloc block4 and block8, if not enough, alloc custom block
func (b *blockPool) alignedBlock(n int) ([]byte, int) {
	if n < block4 {
		return b.block4, 4
	}

	if n < block8 {
		return b.block8, 8
	}

	nums := n / block4096
	remain := n % block4096
	if remain > 0 {
		nums++
	}

	return alignedCustomBlock(nums), nums
}

func (b *blockPool) recycleBlock(block []byte) {
	blockType := len(block)
	for i := 0; i < blockType; i++ {
		block[i] = 0
	}
	switch blockType {
	case block4:
		b.block4 = block
	case block8:
		b.block8 = block
	default:
		log.Panic("block type error")
	}
}

func alignedCustomBlock(blockNums int) []byte {
	block := make([]byte, block4096*blockNums)
	if isAligned(block) {
		return block
	} else {
		block = make([]byte, block4096*blockNums+alignSize)
	}

	a := alignment(block, alignSize)
	offset := 0
	if a != 0 {
		offset = alignSize - a
	}
	block = block[offset : offset+block4096]

	if !isAligned(block) {
		log.Panic("Failed to align block")
	}
	return block
}

func alignment(block []byte, alignSize int) int {
	block0Addr := uintptr(unsafe.Pointer(&block[0]))
	align := uintptr(alignSize - 1)
	return int(block0Addr & align)
}

func isAligned(block []byte) bool {
	return alignment(block, alignSize) == 0
}
