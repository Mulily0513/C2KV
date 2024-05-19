package app

import (
	"context"
	"github.com/Mulily0513/C2KV/api/c2kvserverpb"
)

type MaintainService struct {
	kvService *KvService
	c2kvserverpb.UnimplementedMaintenanceServer
}

func (cs *MaintainService) Status(ctx context.Context, req *c2kvserverpb.StatusRequest) (*c2kvserverpb.StatusResponse, error) {
	status := cs.kvService.RaftNode.Status()
	return &c2kvserverpb.StatusResponse{
		IsLeader: status.IsLeader,
	}, nil
}
