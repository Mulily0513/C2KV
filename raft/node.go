package raft

import (
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"strings"
)

//go:generate mockgen -source=./raft_node.go -destination=../mocks/raft_node.go -package=mock

type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick()

	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	Propose(data []byte)

	ReportUnreachable(id uint64)

	//Status
	Status() Status

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(msg *pb.Message)

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	Ready() <-chan Ready
	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance()

	// Stop performs any necessary termination of the Node.
	Stop()
}

type Status struct {
	IsLeader bool
}

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a SoftState) equal(b SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

type raftNode struct {
	raft       *raft
	prevSoftSt SoftState
	prevHardSt pb.HardState
	readyC     chan Ready
	propC      chan *pb.Message
	receiveC   chan *pb.Message

	advanceC chan struct{}
	tickC    chan struct{}
}

func StartRaftNode(id uint64, raftConfig *config.RaftConfig, storage db.Storage) Node {
	opts := &raftOpts{
		Id:               id,
		electionTimeout:  raftConfig.ElectionTick,
		heartbeatTimeout: raftConfig.HeartbeatTick,
		storage:          storage,
		peers:            raftConfig.GetPeerIds(),
	}

	rn := &raftNode{
		raft:     newRaft(opts),
		propC:    make(chan *pb.Message),
		receiveC: make(chan *pb.Message),
		readyC:   make(chan Ready),
		advanceC: make(chan struct{}),
		// make tickC a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickC: make(chan struct{}, 128),
	}
	rn.prevSoftSt = rn.raft.softState()
	rn.prevHardSt = rn.raft.hardState()
	go rn.serveAppNode()
	log.Infof("start raft node(id:%x lead:%d state:%s) success,  peers(%s), term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d",
		rn.raft.id, rn.raft.lead, rn.raft.state, strings.Join(rn.raft.trk.VoterNodesStr(), ","), rn.raft.Term, rn.raft.raftLog.committed,
		rn.raft.raftLog.applied, rn.raft.raftLog.lastIndex(), rn.raft.raftLog.lastTerm())
	return rn
}

func (rn *raftNode) serveAppNode() {
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	for {
		//If advanceC is not nil, it means that at this time it is waiting for the application layer to process the previous ready.
		//At this time, a new ready cannot be sent to the application layer, so readyC is set to nil.
		if advanceC != nil {
			readyC = nil
		} else if advanceC == nil && rn.HasReady() {
			rd = rn.newReady()
			readyC = rn.readyC
		}

		select {
		case <-rn.tickC:
			rn.raft.tick()

			//msg from peers
		case m := <-rn.receiveC:
			rn.raft.Step(m)
			//msg from client
		case m := <-rn.propC:
			rn.raft.Step(m)

		case readyC <- rd:
			advanceC = rn.advanceC
		case <-advanceC:
			rn.advance(rd)
			rd = Ready{}
			advanceC = nil
		}
	}
}

func (rn *raftNode) Tick() {
	select {
	case rn.tickC <- struct{}{}:
	default:
		log.Warnf("%x A tick missed to fire. Node blocks too long!", rn.raft.id)
	}
}

func (rn *raftNode) Status() (status Status) {
	state := rn.raft.softState()
	if state.RaftState == StateLeader {
		status.IsLeader = true
	}
	return
}

func (rn *raftNode) Step(m *pb.Message) {
	if m.Type == pb.MsgBeat || m.Type == pb.MsgHup {
		return
	}
	if m.Type == pb.MsgProp {
		rn.propC <- m
	} else {
		rn.receiveC <- m
	}
	return
}

func (rn *raftNode) Propose(data []byte) {
	rn.Step(&pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (rn *raftNode) ReportUnreachable(id uint64) {
	//TODO implement me
	panic("implement me")
}

func (rn *raftNode) Ready() <-chan Ready { return rn.readyC }

func (rn *raftNode) Advance() { rn.advanceC <- struct{}{} }

func (rn *raftNode) Stop() {
	//todo
}

func (rn *raftNode) HasReady() bool {
	r := rn.raft
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstableEntries()) > 0 || r.raftLog.hasNextCommittedEnts() {
		return true
	}
	return false
}

func (rn *raftNode) advance(rd Ready) {
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	rn.raft.advance(rd)
}

func (rn *raftNode) newReady() Ready {
	rd := Ready{
		UnstableEntries:  rn.raft.raftLog.unstableEntries(),
		CommittedEntries: rn.raft.raftLog.nextCommittedEnts(),
		Messages:         rn.raft.msgs,
	}
	if softSt := rn.raft.softState(); !softSt.equal(rn.prevSoftSt) {
		rd.SoftState = softSt
	}
	rn.prevSoftSt = rd.SoftState
	if hardSt := rn.raft.hardState(); !isHardStateEqual(hardSt, rn.prevHardSt) {
		rd.HardState = hardSt
	}
	//clear msg
	rn.raft.msgs = nil
	return rd
}

type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	SoftState

	HardState pb.HardState

	//ConfState raftpb.ConfState

	//  specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	UnstableEntries []*pb.Entry

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []*pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []*pb.Message
}
