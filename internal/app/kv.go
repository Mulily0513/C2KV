package app

import (
	"context"
	"errors"
	c2kvserverpb2 "github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
	"github.com/Mulily0513/C2KV/internal/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVService struct {
	kvService *KvService
	c2kvserverpb2.UnimplementedKVServer
}

func (kv *KVService) Put(ctx context.Context, req *c2kvserverpb2.PutRequest) (*c2kvserverpb2.PutResponse, error) {
	if err := kv.ensureLeader(); err != nil {
		return nil, err
	}
	if err := kv.kvService.Propose(ctx, req.Key, req.Value, false); err != nil {
		return nil, mapProposalError(err)
	}
	return &c2kvserverpb2.PutResponse{Msg: "ok"}, nil
}

func (kv *KVService) Range(ctx context.Context, req *c2kvserverpb2.RangeRequest) (*c2kvserverpb2.RangeResponse, error) {
	if err := validateRangeRequest(req); err != nil {
		return nil, err
	}
	// Keep default reads linearizable (leader path), and allow explicit stale reads on followers.
	linearizable := !req.Serializable
	if linearizable {
		if err := kv.ensureLeader(); err != nil {
			return nil, err
		}
	}

	if len(req.RangeEnd) == 0 {
		bkv, err := kv.kvService.Lookup(req.Key)
		if err != nil {
			if err == code.ErrRecordNotExists {
				if linearizable {
					// During leader transition, a stale local read can transiently look
					// like "not found". Re-check leadership so client retries instead
					// of counting a false miss.
					if leaderErr := kv.ensureLeader(); leaderErr != nil {
						return nil, leaderErr
					}
				}
				return &c2kvserverpb2.RangeResponse{Kvs: nil}, nil
			}
			return nil, err
		}
		return &c2kvserverpb2.RangeResponse{Kvs: []*c2kvserverpb2.KeyValue{{Key: bkv.Key, Value: bkv.Value}}}, nil
	}

	kvs, err := kv.kvService.Scan(req.Key, req.RangeEnd)
	if err != nil {
		return nil, err
	}
	if linearizable && len(kvs) == 0 {
		// Same protection for range reads under role transition.
		if err := kv.ensureLeader(); err != nil {
			return nil, err
		}
	}

	if req.Limit > 0 && int64(len(kvs)) > req.Limit {
		kvs = kvs[:req.Limit]
	}
	if req.CountOnly {
		return &c2kvserverpb2.RangeResponse{Kvs: nil}, nil
	}
	pbKVs := make([]*c2kvserverpb2.KeyValue, 0, len(kvs))
	for _, item := range kvs {
		if req.KeysOnly {
			pbKVs = append(pbKVs, &c2kvserverpb2.KeyValue{Key: item.Key})
		} else {
			pbKVs = append(pbKVs, &c2kvserverpb2.KeyValue{Key: item.Key, Value: item.Value})
		}
	}
	return &c2kvserverpb2.RangeResponse{Kvs: pbKVs}, nil
}

func validateRangeRequest(req *c2kvserverpb2.RangeRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "range request is nil")
	}
	if req.Revision != 0 {
		return status.Error(codes.InvalidArgument, "range revision is not supported")
	}
	if req.SortOrder != c2kvserverpb2.RangeRequest_NONE {
		return status.Error(codes.InvalidArgument, "range sort order is not supported")
	}
	if req.SortTarget != c2kvserverpb2.RangeRequest_KEY {
		return status.Error(codes.InvalidArgument, "range sort target is not supported")
	}
	if req.MinModRevision != 0 || req.MaxModRevision != 0 || req.MinCreateRevision != 0 || req.MaxCreateRevision != 0 {
		return status.Error(codes.InvalidArgument, "range revision filters are not supported")
	}
	return nil
}

func (kv *KVService) DeleteRange(ctx context.Context, req *c2kvserverpb2.DeleteRangeRequest) (*c2kvserverpb2.DeleteRangeResponse, error) {
	if err := kv.ensureLeader(); err != nil {
		return nil, err
	}
	deleted, prev, err := kv.kvService.DeleteRange(ctx, req.Key, req.RangeEnd, req.PrevKv)
	if err != nil {
		return nil, mapProposalError(err)
	}
	resp := &c2kvserverpb2.DeleteRangeResponse{Deleted: deleted}
	if req.PrevKv {
		resp.PrevKvs = make([]*c2kvserverpb2.KeyValue, 0, len(prev))
		for _, item := range prev {
			resp.PrevKvs = append(resp.PrevKvs, &c2kvserverpb2.KeyValue{
				Key:   item.Key,
				Value: item.Value,
			})
		}
	}
	return resp, nil
}

func (kv *KVService) ensureLeader() error {
	if kv == nil || kv.kvService == nil || kv.kvService.RaftNode == nil {
		return status.Error(codes.FailedPrecondition, "raft node unavailable")
	}
	if !kv.kvService.RaftNode.Status().IsLeader {
		return status.Error(codes.FailedPrecondition, "not leader")
	}
	return nil
}

func mapProposalError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrRequestTimeout) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
