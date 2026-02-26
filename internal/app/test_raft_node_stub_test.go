package app

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/raft"
)

type staticStatusNode struct {
	isLeader bool
}

func newStaticStatusNode(isLeader bool) *staticStatusNode {
	return &staticStatusNode{isLeader: isLeader}
}

func (n *staticStatusNode) Tick() {}

func (n *staticStatusNode) Propose(_ []byte) {}

func (n *staticStatusNode) ReportUnreachable(_ uint64) {}

func (n *staticStatusNode) Status() raft.Status {
	return raft.Status{IsLeader: n.isLeader}
}

func (n *staticStatusNode) Step(_ *pb.Message) {}

func (n *staticStatusNode) Ready() <-chan raft.Ready { return nil }

func (n *staticStatusNode) Advance() {}

func (n *staticStatusNode) Stop() {}
