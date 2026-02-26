package app

import (
	"context"
	"errors"
	c2kvserverpb "github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/golang/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

func TestKVServicePutRejectsNonLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &KvService{
		Storage:    mocks.NewMockStorage(ctrl),
		proposeC:   make(chan []byte, 1),
		monitor:    newProposalMonitor(),
		ReqTimeout: 100 * time.Millisecond,
		RaftNode:   newStaticStatusNode(false),
	}
	grpcSvc := &KVService{kvService: svc}

	_, err := grpcSvc.Put(context.Background(), &c2kvserverpb.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got err=%v code=%v", err, status.Code(err))
	}
}

func TestKVServicePutTimeoutWhenNotApplied(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &KvService{
		Storage:    mocks.NewMockStorage(ctrl),
		proposeC:   make(chan []byte, 1),
		monitor:    newProposalMonitor(),
		ReqTimeout: 20 * time.Millisecond,
		RaftNode:   newStaticStatusNode(true),
	}
	grpcSvc := &KVService{kvService: svc}

	_, err := grpcSvc.Put(context.Background(), &c2kvserverpb.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got err=%v code=%v", err, status.Code(err))
	}
}

func TestKvServiceProposeContextCancelCleansMonitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &KvService{
		Storage:    mocks.NewMockStorage(ctrl),
		proposeC:   make(chan []byte),
		monitor:    newProposalMonitor(),
		ReqTimeout: time.Second,
		RaftNode:   newStaticStatusNode(true),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := svc.Propose(ctx, []byte("k"), []byte("v"), false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	if got := svc.monitor.len(); got != 0 {
		t.Fatalf("monitor map should be empty after cancellation, got %d", got)
	}
}
