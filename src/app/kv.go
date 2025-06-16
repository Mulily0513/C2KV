package app

import (
	"context"
	c2kvserverpb2 "github.com/Mulily0513/C2KV/c2kvserverpb"
)

type KVService struct {
	kvService *KvService
	c2kvserverpb2.UnimplementedKVServer
}

func (kv *KVService) Put(ctx context.Context, req *c2kvserverpb2.PutRequest) (*c2kvserverpb2.PutResponse, error) {
	if err := kv.kvService.Propose(req.Key, req.Value, false); err != nil {
		return &c2kvserverpb2.PutResponse{Msg: "false"}, nil
	}
	return &c2kvserverpb2.PutResponse{Msg: "ok"}, nil
}

func (kv *KVService) Range(ctx context.Context, req *c2kvserverpb2.RangeRequest) (*c2kvserverpb2.RangeResponse, error) {
	if req.RangeEnd == nil {
		kv, err := kv.kvService.Lookup(req.Key)
		if err != nil {
			return nil, err
		}
		return &c2kvserverpb2.RangeResponse{Kvs: []*c2kvserverpb2.KeyValue{&c2kvserverpb2.KeyValue{Key: kv.Key, Value: kv.Value}}}, nil
	}
	return nil, nil
}

func (kv *KVService) Delete(ctx context.Context, req *c2kvserverpb2.PutRequest) (*c2kvserverpb2.PutResponse, error) {
	if err := kv.kvService.Propose(req.Key, req.Value, true); err != nil {
		return &c2kvserverpb2.PutResponse{Msg: "false"}, nil
	}
	return &c2kvserverpb2.PutResponse{Msg: "ok"}, nil
}
