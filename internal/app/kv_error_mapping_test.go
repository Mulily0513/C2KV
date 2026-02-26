package app

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestKVServiceEnsureLeader(t *testing.T) {
	tests := []struct {
		name string
		kv   *KVService
		want codes.Code
	}{
		{
			name: "nil kv service",
			kv:   &KVService{},
			want: codes.FailedPrecondition,
		},
		{
			name: "nil raft node",
			kv: &KVService{
				kvService: &KvService{},
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "follower",
			kv: &KVService{
				kvService: &KvService{RaftNode: newStaticStatusNode(false)},
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "leader",
			kv: &KVService{
				kvService: &KvService{RaftNode: newStaticStatusNode(true)},
			},
			want: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.kv.ensureLeader()
			if tt.want == codes.OK {
				if err != nil {
					t.Fatalf("ensureLeader returned unexpected error: %v", err)
				}
				return
			}
			if status.Code(err) != tt.want {
				t.Fatalf("unexpected code, want=%v got=%v err=%v", tt.want, status.Code(err), err)
			}
		})
	}
}

func TestMapProposalError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "nil", err: nil, want: codes.OK},
		{name: "context canceled", err: context.Canceled, want: codes.Canceled},
		{name: "deadline", err: context.DeadlineExceeded, want: codes.DeadlineExceeded},
		{name: "request timeout", err: ErrRequestTimeout, want: codes.DeadlineExceeded},
		{name: "other", err: errors.New("x"), want: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapProposalError(tt.err)
			if tt.want == codes.OK {
				if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
				return
			}
			if status.Code(err) != tt.want {
				t.Fatalf("unexpected code, want=%v got=%v err=%v", tt.want, status.Code(err), err)
			}
		})
	}
}
