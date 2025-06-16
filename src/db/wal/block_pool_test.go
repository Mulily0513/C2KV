package wal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func mockData(block []byte) {
	for i := 0; i < len(block); i++ {
		block[i] = 1
	}
}

func TestAlignedBlock(t *testing.T) {
	blockPool := newBlockPool()

	block1, count1 := blockPool.alignedBlock(1)
	assert.Equal(t, isAligned(block1), true)
	assert.Equal(t, block4096*4, len(block1))
	assert.Equal(t, 4, count1)

	block2, count2 := blockPool.alignedBlock(block4096*4 + 1)
	assert.Equal(t, isAligned(block2), true)
	assert.Equal(t, len(block2), block4096*8)
	assert.Equal(t, count2, 8)

	block3, count3 := blockPool.alignedBlock(block4096*8 + 1)
	assert.Equal(t, isAligned(block3), true)
	assert.Equal(t, block4096*9, len(block3))
	assert.Equal(t, 9, count3)
}

func TestBlockPool_RecycleBlock(t *testing.T) {
	blockPool := newBlockPool()
	block, _ := blockPool.alignedBlock(1)
	assert.Equal(t, isAligned(block), true)
	mockData(block)
	blockPool.recycleBlock(block)
	newBlock, _ := blockPool.alignedBlock(1)
	assert.Equal(t, &newBlock[0], &block[0])
}
