package raft

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db"
	"github.com/Mulily0513/C2KV/internal/log"
	"strings"
)

const (
	propChanBufferSize = 16384
	propDrainBatchSize = 1024
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
		maxInflightMsgs:  raftConfig.NormalizedMaxInflightMsg(),
		maxSizePerMsg:    raftConfig.NormalizedMaxSizePerMsg(),
	}

	rn := &raftNode{
		raft:     newRaft(opts),
		propC:    make(chan *pb.Message, propChanBufferSize),
		receiveC: make(chan *pb.Message, propChanBufferSize),
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
		if advanceC == nil {
			rn.drainPendingProposals()
		}
		if advanceC == nil && rn.HasReady() {
			rd = rn.readyWithoutAccept()
			readyC = rn.readyC
		}

		select {
		case readyC <- rd:
			rn.acceptReady(rd)
			readyC = nil
			advanceC = rn.advanceC

		case <-rn.tickC:
			rn.raft.tick()

		//msg from peers
		case m := <-rn.receiveC:
			rn.raft.Step(m)
		//msg from client
		case m := <-rn.propC:
			rn.raft.Step(m)
			rn.drainPendingProposals()

		case <-advanceC:
			rn.advance(rd)
			rd = Ready{}
			advanceC = nil
		}
	}
}

func (rn *raftNode) drainPendingProposals() {
	for i := 0; i < propDrainBatchSize; i++ {
		select {
		case m := <-rn.propC:
			if m == nil {
				continue
			}
			rn.raft.Step(m)
		default:
			return
		}
	}
}

func (rn *raftNode) Tick() {
	select {
	case rn.tickC <- struct{}{}:
	default:
		// Drop overflow ticks under sustained backpressure.
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
	if id == 0 {
		return
	}
	select {
	case rn.receiveC <- &pb.Message{Type: pb.MsgUnreachable, From: id}:
	default:
		// Drop overflow unreachable notifications under heavy pressure.
	}
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
	rn.raft.advance(rd)
}

func (rn *raftNode) readyWithoutAccept() Ready {
	unstable := rn.raft.raftLog.unstableEntries()
	if len(unstable) > 0 {
		unstable = append([]*pb.Entry(nil), unstable...)
	}
	committed := rn.raft.raftLog.nextCommittedEnts()
	if len(committed) > 0 {
		committed = append([]*pb.Entry(nil), committed...)
	}
	msgs := rn.raft.msgs
	if len(msgs) > 0 {
		msgs = append([]*pb.Message(nil), msgs...)
	}
	rd := Ready{
		UnstableEntries:  unstable,
		CommittedEntries: committed,
		Messages:         msgs,
	}
	hardSt := rn.raft.hardState()
	rd.MustSync = MustSync(hardSt, rn.prevHardSt, len(rd.UnstableEntries))
	softSt := rn.raft.softState()
	if !softSt.equal(rn.prevSoftSt) {
		escapingSoftSt := softSt
		rd.SoftState = &escapingSoftSt
	}
	if !isHardStateEqual(hardSt, rn.prevHardSt) {
		rd.HardState = hardSt
	}
	return rd
}

func (rn *raftNode) acceptReady(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = *rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	rn.raft.msgs = nil
}

type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	SoftState *SoftState

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

	// MustSync indicates whether the current Ready requires a durability barrier
	// before messages are sent.
	MustSync bool
}

// MustSync returns true if the hard state and unstable entries indicate that a
// synchronous write to persistent storage is required before sending messages.
func MustSync(st, prevst pb.HardState, entsNum int) bool {
	return entsNum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
