package app

import (
	"context"
	c2kvserverpb2 "github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
	"github.com/Mulily0513/C2KV/internal/telemetry"
)

type MaintainService struct {
	kvService *KvService
	c2kvserverpb2.UnimplementedMaintenanceServer
}

func (cs *MaintainService) Status(ctx context.Context, req *c2kvserverpb2.StatusRequest) (*c2kvserverpb2.StatusResponse, error) {
	status := cs.kvService.RaftNode.Status()
	return &c2kvserverpb2.StatusResponse{
		Name:     telemetry.EncodeStatusName(),
		IsLeader: status.IsLeader,
	}, nil
}
