package raft

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	tracker2 "github.com/Mulily0513/C2KV/internal/raft/tracker"
	"testing"
)

func TestStepIgnoresStaleAppRespTerm(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.becomeCandidate()
	r.becomeLeader()

	pr := r.trk.Progress[2]
	beforeMatch, beforeNext := pr.Match, pr.Next

	r.Step(&pb.Message{
		From:  2,
		To:    1,
		Type:  pb.MsgAppResp,
		Term:  r.Term - 1, // stale remote term (>0)
		Index: 128,
	})

	if pr.Match != beforeMatch || pr.Next != beforeNext {
		t.Fatalf("stale MsgAppResp should be ignored, before=(%d,%d) after=(%d,%d)", beforeMatch, beforeNext, pr.Match, pr.Next)
	}
}

func TestSendAppendClampsOutOfRangeNext(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntry(pb.Entry{Data: []byte("x")})

	lastIndex := r.raftLog.lastIndex()
	pr := r.trk.Progress[2]
	pr.Next = lastIndex + 100

	r.sendAppend(2)
	if pr.Next != lastIndex+1 {
		t.Fatalf("sendAppend should clamp next to lastIndex+1, got=%d want=%d", pr.Next, lastIndex+1)
	}
}

func TestNewReadyKeepsPreviousSoftStateWhenUnchanged(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	rn := &raftNode{
		raft:       r,
		prevSoftSt: r.softState(),
		prevHardSt: r.hardState(),
	}

	_ = rn.readyWithoutAccept()
	if !rn.prevSoftSt.equal(r.softState()) {
		t.Fatalf("prevSoftSt must remain current soft state, got=%+v want=%+v", rn.prevSoftSt, r.softState())
	}
}

func TestReadyWithoutAcceptKeepsMessagesUntilAccept(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	rn := &raftNode{
		raft:       r,
		prevSoftSt: r.softState(),
		prevHardSt: r.hardState(),
	}
	r.msgs = append(r.msgs, &pb.Message{Type: pb.MsgHeartbeat, From: 1, To: 2})

	rd := rn.readyWithoutAccept()
	if len(rd.Messages) != 1 {
		t.Fatalf("ready should include pending message, got=%d", len(rd.Messages))
	}
	if len(r.msgs) != 1 {
		t.Fatalf("readyWithoutAccept must not clear raft msgs, got=%d", len(r.msgs))
	}

	rn.acceptReady(rd)
	if len(r.msgs) != 0 {
		t.Fatalf("acceptReady must clear raft msgs, got=%d", len(r.msgs))
	}
}

func TestAcceptReadyUpdatesPrevSoftState(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	rn := &raftNode{
		raft:       r,
		prevSoftSt: r.softState(),
		prevHardSt: r.hardState(),
	}
	r.lead = 2

	rd := rn.readyWithoutAccept()
	if rd.SoftState == nil {
		t.Fatal("soft state change should be exposed in ready")
	}
	if rn.prevSoftSt.equal(*rd.SoftState) {
		t.Fatal("readyWithoutAccept must not mutate prevSoftSt before accept")
	}

	rn.acceptReady(rd)
	if !rn.prevSoftSt.equal(*rd.SoftState) {
		t.Fatalf("acceptReady must update prevSoftSt, got=%+v want=%+v", rn.prevSoftSt, *rd.SoftState)
	}
}

func TestSendAppendReplicateOptimisticallyAdvancesNext(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntry(pb.Entry{Data: []byte("a")}, pb.Entry{Data: []byte("b")})

	pr := r.trk.Progress[2]
	pr.State = tracker2.StateReplicate
	pr.Next = 1

	r.sendAppend(2)
	if len(r.msgs) != 1 {
		t.Fatalf("expected one append msg, got=%d", len(r.msgs))
	}
	lastSent := r.msgs[0].Entries[len(r.msgs[0].Entries)-1].Index
	if pr.Next != lastSent+1 {
		t.Fatalf("replicate next should advance optimistically, got=%d want=%d", pr.Next, lastSent+1)
	}
}

func TestSendAppendProbePauseUntilAck(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntry(pb.Entry{Data: []byte("x")})

	pr := r.trk.Progress[2]
	pr.State = tracker2.StateProbe
	pr.Next = 1

	r.sendAppend(2)
	if !pr.ProbeSent {
		t.Fatal("probe append must set ProbeSent")
	}
	firstMsgs := len(r.msgs)
	r.sendAppend(2)
	if len(r.msgs) != firstMsgs {
		t.Fatalf("probe pause should prevent duplicate append while waiting ack, before=%d after=%d", firstMsgs, len(r.msgs))
	}
}

func TestHandleHeartbeatClampsCommitToLastIndex(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntry(pb.Entry{Data: []byte("x")})
	lastIndex := r.raftLog.lastIndex()
	if lastIndex == 0 {
		t.Fatal("expected non-zero last index")
	}
	r.becomeFollower(r.Term, 2)
	r.raftLog.committed = 0

	overshoot := lastIndex + 100
	r.handleHeartbeat(&pb.Message{
		From:   2,
		To:     1,
		Type:   pb.MsgHeartbeat,
		Term:   r.Term,
		Commit: overshoot,
	})

	if r.raftLog.committed != lastIndex {
		t.Fatalf("heartbeat commit must be clamped, got=%d want=%d", r.raftLog.committed, lastIndex)
	}
	if len(r.msgs) == 0 || r.msgs[len(r.msgs)-1].Type != pb.MsgHeartbeatResp {
		t.Fatalf("expected heartbeat response, msgs=%+v", r.msgs)
	}
}

func TestAdvanceIgnoresStaleCommittedEntries(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.raftLog.applied = 10
	r.raftLog.committed = 12

	r.advance(Ready{
		CommittedEntries: []*pb.Entry{
			{Index: 9},
		},
	})

	if r.raftLog.applied != 10 {
		t.Fatalf("stale advance must not decrease applied, got=%d want=10", r.raftLog.applied)
	}
}

func TestSendAppendRespectsMaxInflightWindow(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntry(
		pb.Entry{Data: []byte("a")},
		pb.Entry{Data: []byte("b")},
		pb.Entry{Data: []byte("c")},
		pb.Entry{Data: []byte("d")},
	)

	pr := r.trk.Progress[2]
	pr.State = tracker2.StateReplicate
	pr.Next = 1
	pr.MaxInflight = 2
	all, err := r.raftLog.slice(1, r.raftLog.lastIndex()+1)
	if err != nil {
		t.Fatalf("slice failed: %v", err)
	}
	r.maxSizePerMsg = uint64(all[0].Size())

	r.sendAppend(2)
	r.sendAppend(2)
	r.sendAppend(2)

	if len(r.msgs) != 2 {
		t.Fatalf("max inflight window must cap outstanding append msgs, got=%d want=2", len(r.msgs))
	}
	if pr.Inflight != 2 {
		t.Fatalf("inflight counter mismatch, got=%d want=2", pr.Inflight)
	}

	lastIdx := r.msgs[0].Entries[len(r.msgs[0].Entries)-1].Index
	r.handleAppendResponse(&pb.Message{From: 2, Type: pb.MsgAppResp, Index: lastIdx, Term: r.Term})
	if pr.Inflight > pr.MaxInflight {
		t.Fatalf("append inflight must stay within window, inflight=%d max=%d", pr.Inflight, pr.MaxInflight)
	}
	if len(r.msgs) <= 2 {
		t.Fatalf("ack should trigger additional append traffic, got=%d want>2", len(r.msgs))
	}
}

func TestSendAppendRespectsMaxSizePerMsg(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntry(
		pb.Entry{Data: []byte("entry-1")},
		pb.Entry{Data: []byte("entry-2")},
		pb.Entry{Data: []byte("entry-3")},
	)

	pr := r.trk.Progress[2]
	pr.State = tracker2.StateReplicate
	pr.Next = 1

	all, err := r.raftLog.slice(1, r.raftLog.lastIndex()+1)
	if err != nil {
		t.Fatalf("slice failed: %v", err)
	}
	if len(all) < 3 {
		t.Fatalf("expected at least 3 entries, got=%d", len(all))
	}
	r.maxSizePerMsg = uint64(all[0].Size() + all[1].Size())

	r.sendAppend(2)
	if len(r.msgs) != 1 {
		t.Fatalf("expected one append message, got=%d", len(r.msgs))
	}
	if got := len(r.msgs[0].Entries); got != 2 {
		t.Fatalf("maxSizePerMsg should cap entries per append, got=%d want=2", got)
	}
}
