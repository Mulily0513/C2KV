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
		return &c2kvserverpb.PutResponse{Msg: "ok"}, nil
	}
	return &c2kvserverpb.PutResponse{Msg: "false"}, nil
}

func (kv *KVService) Delete(ctx context.Context, req *c2kvserverpb.PutRequest) (*c2kvserverpb.PutResponse, error) {
	if err := kv.kvService.Propose(req.Key, req.Value, true); err != nil {
		return &c2kvserverpb.PutResponse{Msg: "ok"}, nil
	}
	return &c2kvserverpb.PutResponse{Msg: "false"}, nil
}
