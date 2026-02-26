package raft

import (
	"testing"

	"github.com/Mulily0513/C2KV/api/gen/pb"
)

func TestMustSync(t *testing.T) {
	base := pb.HardState{Term: 2, Vote: 1, Commit: 10}
	tests := []struct {
		name   string
		st     pb.HardState
		prev   pb.HardState
		ents   int
		expect bool
	}{
		{
			name:   "no entries and no term vote change",
			st:     base,
			prev:   base,
			ents:   0,
			expect: false,
		},
		{
			name:   "entries require sync",
			st:     base,
			prev:   base,
			ents:   1,
			expect: true,
		},
		{
			name:   "term change requires sync",
			st:     pb.HardState{Term: 3, Vote: 1, Commit: 10},
			prev:   base,
			ents:   0,
			expect: true,
		},
		{
			name:   "vote change requires sync",
			st:     pb.HardState{Term: 2, Vote: 3, Commit: 10},
			prev:   base,
			ents:   0,
			expect: true,
		},
		{
			name:   "commit change only does not require sync",
			st:     pb.HardState{Term: 2, Vote: 1, Commit: 11},
			prev:   base,
			ents:   0,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MustSync(tt.st, tt.prev, tt.ents)
			if got != tt.expect {
				t.Fatalf("MustSync() mismatch, got=%v want=%v", got, tt.expect)
			}
		})
	}
}
