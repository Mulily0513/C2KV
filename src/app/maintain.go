package app

import (
	"context"
	c2kvserverpb2 "github.com/Mulily0513/C2KV/c2kvserverpb"
)

type MaintainService struct {
	kvService *KvService
	c2kvserverpb2.UnimplementedMaintenanceServer
}

func (cs *MaintainService) Status(ctx context.Context, req *c2kvserverpb2.StatusRequest) (*c2kvserverpb2.StatusResponse, error) {
	status := cs.kvService.RaftNode.Status()
	return &c2kvserverpb2.StatusResponse{
		IsLeader: status.IsLeader,
	}, nil
}
