package mocks

import (
	"github.com/Mulily0513/C2KV/internal/config"
)

var TestMemConfig = config.MemConfig{
	MemTableSize: 64,
	Concurrency:  8,
}
