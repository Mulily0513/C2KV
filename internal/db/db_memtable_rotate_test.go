package db

import (
	"github.com/Mulily0513/C2KV/internal/config"
	"testing"
)

func TestMaybeRotateMemTableUsesMBUnit(t *testing.T) {
	cfg := config.MemConfig{MemTableSize: 1, Concurrency: 1}
	active := newMemTable(cfg)
	next := newMemTable(cfg)

	database := &C2KV{
		activeMem:    active,
		immtableQ:    newMemTableQueue(2),
		memFlushC:    make(chan *memTable, 2),
		memTablePipe: make(chan *memTable, 1),
	}
	database.memTablePipe <- next

	database.maybeRotateMemTable(2 * 1024)

	if database.activeMem != active {
		t.Fatalf("unexpected rotate when bytes are below 1MB")
	}
	if database.immtableQ.size != 0 {
		t.Fatalf("unexpected immutable memtable enqueue, size=%d", database.immtableQ.size)
	}
}

func TestMaybeRotateMemTableWhenExceedLimit(t *testing.T) {
	cfg := config.MemConfig{MemTableSize: 1, Concurrency: 1}
	active := newMemTable(cfg)
	next := newMemTable(cfg)

	database := &C2KV{
		activeMem:    active,
		immtableQ:    newMemTableQueue(2),
		memFlushC:    make(chan *memTable, 2),
		memTablePipe: make(chan *memTable, 1),
	}
	database.memTablePipe <- next

	database.maybeRotateMemTable(2 * MB)

	if database.activeMem != next {
		t.Fatalf("expected rotate when bytes exceed 1MB")
	}
	if database.immtableQ.size != 1 {
		t.Fatalf("expected one immutable memtable, got %d", database.immtableQ.size)
	}
}
