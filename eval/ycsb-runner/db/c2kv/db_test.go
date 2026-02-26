package c2kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magiconair/properties"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSplitAndTrim(t *testing.T) {
	got := splitAndTrim(" 127.0.0.1:2341, ,127.0.0.1:2342 ,, 127.0.0.1:2343 ")
	want := []string{"127.0.0.1:2341", "127.0.0.1:2342", "127.0.0.1:2343"}
	if len(got) != len(want) {
		t.Fatalf("endpoint length mismatch, want=%d got=%d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected endpoint at %d, want=%q got=%q", i, want[i], got[i])
		}
	}
}

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unavailable", err: status.Error(codes.Unavailable, "x"), want: true},
		{name: "failed precondition", err: status.Error(codes.FailedPrecondition, "x"), want: true},
		{name: "deadline exceeded", err: status.Error(codes.DeadlineExceeded, "x"), want: true},
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "x"), want: false},
		{name: "plain error", err: errors.New("x"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRetry(tt.err); got != tt.want {
				t.Fatalf("shouldRetry mismatch, want=%v got=%v err=%v", tt.want, got, tt.err)
			}
		})
	}
}

func TestCreatorCreateRejectsEmptyEndpoints(t *testing.T) {
	p := properties.NewProperties()
	p.Set(c2kvEndpoints, " , , ")

	_, err := (c2kvCreator{}).Create(p)
	if err == nil {
		t.Fatal("expected create to fail on empty endpoint list")
	}
}

func TestWithTimeout(t *testing.T) {
	db := &c2kvDB{requestTimeout: 40 * time.Millisecond}
	ctx, cancel := db.withTimeout(context.Background())
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected timeout context to contain a deadline")
	}
	if time.Until(deadline) <= 0 {
		t.Fatalf("expected deadline in the future, got %v", deadline)
	}
}

func TestWithTimeoutZeroDisablesDeadline(t *testing.T) {
	db := &c2kvDB{requestTimeout: 0}
	ctx, cancel := db.withTimeout(context.Background())
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Fatal("did not expect deadline when request timeout is zero")
	}
}

func TestRowKey(t *testing.T) {
	got := rowKey("usertable", "user1")
	if got != "usertable:user1" {
		t.Fatalf("unexpected row key: %q", got)
	}
}
