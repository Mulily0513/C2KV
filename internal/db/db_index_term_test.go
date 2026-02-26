package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/Mulily0513/C2KV/internal/code"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
)

func TestC2KVEntriesRangeValidation(t *testing.T) {
	db := newApplyTestDB(t, mocks.CreateEntries(8, 32), 0)

	if _, err := db.Entries(0, 2); err == nil || !strings.Contains(err.Error(), "compacted") {
		t.Fatalf("expected compacted error for low bound, got %v", err)
	}
	if _, err := db.Entries(1, 10); err == nil || !strings.Contains(err.Error(), "compacted") {
		t.Fatalf("expected compacted error for high bound, got %v", err)
	}
}

func TestC2KVEntriesReturnsCopy(t *testing.T) {
	db := newApplyTestDB(t, mocks.CreateEntries(5, 32), 0)

	got, err := db.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	originalTerm := got[0].Term
	got[0].Term = 999

	again, err := db.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries second call failed: %v", err)
	}
	if again[0].Term != originalTerm {
		t.Fatalf("db internal entries were mutated by caller, want term=%d got=%d", originalTerm, again[0].Term)
	}
}

func TestC2KVTermUsesAppliedTermAndCompactedBoundary(t *testing.T) {
	db := newApplyTestDB(t, nil, 3)
	db.wal.WalStateSegment.AppliedTerm = 7

	term, err := db.Term(0)
	if err != nil || term != 0 {
		t.Fatalf("Term(0) mismatch, want(0,nil) got(%d,%v)", term, err)
	}
	term, err = db.Term(3)
	if err != nil || term != 7 {
		t.Fatalf("Term(appliedIndex) mismatch, want(7,nil) got(%d,%v)", term, err)
	}
	_, err = db.Term(2)
	if !errors.Is(err, code.ErrCompacted) {
		t.Fatalf("expected ErrCompacted for i < firstIndex, got %v", err)
	}
	_, err = db.Term(4)
	if !errors.Is(err, code.ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable for i > lastIndex, got %v", err)
	}
}

func TestC2KVFirstAndStableIndexWhenEntriesEmpty(t *testing.T) {
	db := newApplyTestDB(t, nil, 11)

	if got := db.FirstIndex(); got != 12 {
		t.Fatalf("FirstIndex mismatch, want=12 got=%d", got)
	}
	if got := db.StableIndex(); got != 11 {
		t.Fatalf("StableIndex mismatch, want=11 got=%d", got)
	}
}
