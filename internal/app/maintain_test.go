package app

import (
	"context"
	"testing"

	c2kvserverpb "github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
	"github.com/Mulily0513/C2KV/internal/telemetry"
)

func TestMaintainServiceStatusIncludesLeaderAndTelemetry(t *testing.T) {
	tests := []struct {
		name     string
		isLeader bool
	}{
		{name: "leader", isLeader: true},
		{name: "follower", isLeader: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := &MaintainService{
				kvService: &KvService{
					RaftNode: newStaticStatusNode(tt.isLeader),
				},
			}

			resp, err := ms.Status(context.Background(), &c2kvserverpb.StatusRequest{})
			if err != nil {
				t.Fatalf("Status returned error: %v", err)
			}
			if resp.IsLeader != tt.isLeader {
				t.Fatalf("unexpected IsLeader, want=%v got=%v", tt.isLeader, resp.IsLeader)
			}

			snapshot, ok, err := telemetry.DecodeStatusName(resp.Name)
			if err != nil {
				t.Fatalf("status name decode failed: %v", err)
			}
			if !ok {
				t.Fatalf("status name is not telemetry payload: %q", resp.Name)
			}
			if snapshot.CapturedAtUnixMilli <= 0 {
				t.Fatalf("invalid snapshot timestamp: %+v", snapshot)
			}
		})
	}
}
