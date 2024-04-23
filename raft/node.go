package raft

import (
	"context"
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

//go:generate mockgen -source=./raft_node.go -destination=../mocks/raft_node.go -package=mock

type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick()
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	Propose(data []byte) error

	ReportUnreachable(id uint64)

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(msg pb.Message) error

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

var (
	emptyState = pb.HardState{}
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

type raftNode struct {
	rawNode  *rawNode
	readyC   chan Ready
	propC    chan msgWithResult
	receiveC chan pb.Message

	advanceC chan struct{}
	tickC    chan struct{}
	doneC    chan struct{}
	stopC    chan struct{}
}

func StartRaftNode(raftConfig *config.RaftConfig, storage db.Storage) Node {
	opts := &raftOpts{
		Id:               raftConfig.Id,
		electionTimeout:  raftConfig.ElectionTick,
		heartbeatTimeout: raftConfig.HeartbeatTick,
		storage:          storage,
	}

	rN := &raftNode{
		propC:    make(chan msgWithResult),
		receiveC: make(chan pb.Message),
		readyC:   make(chan Ready),
		advanceC: make(chan struct{}),
		// make tickC a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickC:   make(chan struct{}, 128),
		doneC:   make(chan struct{}),
		stopC:   make(chan struct{}),
		rawNode: NewRawNode(opts),
	}
	rN.serveAppNode()
	return rN
}

func (rn *raftNode) serveAppNode() {
	var propC chan msgWithResult
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	r := rn.rawNode.raft

	for {
		//advanceC 不为nil，说明此时在等待应用层处理完上轮ready后
		//下发advance命令，不能发送ready到应用层，将readyC置为nil
		if advanceC != nil {
			readyC = nil
		}

		if advanceC == nil && rn.rawNode.HasReady() {
			rd = rn.rawNode.readyWithoutAccept()
			readyC = rn.readyC
		}

		select {
		case <-rn.tickC:
			rn.rawNode.Tick()
		case m := <-rn.receiveC:
			if pr := r.trk.Progress[m.From]; pr != nil {
				r.Step(m)
			}
		case pm := <-propC:
			m := pm.m
			//proposal信息的from为节点id
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}

		case readyC <- rd:
			rn.rawNode.acceptReady(rd)
			advanceC = rn.advanceC
		case <-advanceC:
			rn.rawNode.Advance(rd)
			rd = Ready{}
			advanceC = nil

		case <-rn.stopC:
			close(rn.doneC)
			return
		}
	}
}

func (rn *raftNode) Tick() {
	select {
	case rn.tickC <- struct{}{}:
	default:
		log.Warnf("%x A tick missed to fire. Node blocks too long!", rn.rawNode.raft.id)
	}
}

func (rn *raftNode) Propose(ctx context.Context, data []byte) error {
	return rn.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (rn *raftNode) ReportUnreachable(id uint64) {
	//TODO implement me
	panic("implement me")
}

func (rn *raftNode) Step(ctx context.Context, m pb.Message) error {
	if IsLocalMsg(m.Type) {
		log.Errorf("node %d receive a wrong type msg from other peer  %d,msg type %s", m.To, m.From, m.Type.String())
		return nil
	}
	return rn.step(m)
}

func (rn *raftNode) step(m pb.Message) error {
	return rn.stepWithWaitOption(m, false)
}

func (rn *raftNode) stepWait(m pb.Message) error {
	return rn.stepWithWaitOption(m, true)
}

func (rn *raftNode) stepWithWaitOption(m pb.Message, wait bool) error {
	if m.Type != pb.MsgProp {
		select {
		case rn.receiveC <- m:
			return nil
		case <-rn.doneC:
			return ErrStopped
		}
	}
	ch := rn.propC
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-rn.doneC:
		return ErrStopped
	}

	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-rn.doneC:
		return ErrStopped
	}
	return nil
}

type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []*pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	MustSync bool

	HardState pb.HardState

	ConfState pb.ConfState

	UnstableEntries []*pb.Entry //需要持久化的entries
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextCommittedEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}

	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	return 0
}

func (rn *raftNode) Ready() <-chan Ready { return rn.readyC }

func (rn *raftNode) Advance() { rn.advanceC <- struct{}{} }

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
// MustSync 在这里，"同步写入"指的是将数据立即写入到持久化存储（如磁盘）中，
// 并且在写入完成之前阻塞其他操作，以确保数据的持久性和一致性。
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

func (rn *raftNode) Stop() {
	select {
	case rn.stopC <- struct{}{}:
		// Not already stopped, so trigger it
	case <-rn.doneC:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-rn.doneC
}
