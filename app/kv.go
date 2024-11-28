package app

import (
	"context"
	"github.com/Mulily0513/C2KV/api/c2kvserverpb"
)

type KVService struct {
	kvService *KvService
	c2kvserverpb.UnimplementedKVServer
}

func (kv *KVService) Put(ctx context.Context, req *c2kvserverpb.PutRequest) (*c2kvserverpb.PutResponse, error) {
	if err := kv.kvService.Propose(req.Key, req.Value, false); err != nil {
		return &c2kvserverpb.PutResponse{Msg: "false"}, nil
	}
	return &c2kvserverpb.PutResponse{Msg: "ok"}, nil
}

func (kv *KVService) Range(ctx context.Context, req *c2kvserverpb.RangeRequest) (*c2kvserverpb.RangeResponse, error) {
	if req.RangeEnd == nil {
		kv, err := kv.kvService.Lookup(req.Key)
		if err != nil {
			return nil, err
		}
		return &c2kvserverpb.RangeResponse{Kvs: []*c2kvserverpb.KeyValue{&c2kvserverpb.KeyValue{Key: kv.Key, Value: kv.Value}}}, nil
	}
	return nil, nil
}

func (kv *KVService) Delete(ctx context.Context, req *c2kvserverpb.PutRequest) (*c2kvserverpb.PutResponse, error) {
	if err := kv.kvService.Propose(req.Key, req.Value, true); err != nil {
		return &c2kvserverpb.PutResponse{Msg: "false"}, nil
	}
	return &c2kvserverpb.PutResponse{Msg: "ok"}, nil
}
