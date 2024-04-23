package raft

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/mocks"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/golang/mock/gomock"
	"reflect"
	"sort"
	"testing"
)

type messageSlice []pb.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func RaftOpts(id uint64, peers []uint64, election, heartbeat int, storage db.Storage) *raftOpts {
	return &raftOpts{
		Id:               id,
		peers:            peers,
		electionTimeout:  election,
		heartbeatTimeout: heartbeat,
		storage:          storage,
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage db.Storage) *raft {
	r, err := newRaft(RaftOpts(id, peers, election, heartbeat, storage))
	if err != nil {
		panic(err)
	}
	return r
}

func MockStorage(t *testing.T, appliedIndex, fistIndex, stableIndex, expIdx, expTerm uint64, hs pb.HardState, cs pb.ConfState) db.Storage {
	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	storage.EXPECT().FirstIndex().Return(fistIndex).AnyTimes()
	storage.EXPECT().StableIndex().Return(stableIndex).AnyTimes()
	storage.EXPECT().Term(expIdx).Return(expTerm, nil).AnyTimes()
	storage.EXPECT().InitialState().Return(hs, cs).AnyTimes()
	storage.EXPECT().AppliedIndex().Return(appliedIndex).AnyTimes()
	return storage
}

func commitNoopEntry(r *raft, s db.Storage) {
	if r.state != StateLeader {
		panic("it should only be used when it is the leader")
	}
	r.bcastAppend()
	// simulate the response of MsgApp
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.Type != pb.MsgApp || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	r.readMessages()
	s.PersistUnstableEnts(r.raftLog.unstableEntries())
	r.raftLog.appliedTo(r.raftLog.committed)
	r.raftLog.stableTo(r.raftLog.lastIndex())
}

func acceptAndReply(m pb.Message) pb.Message {
	if m.Type != pb.MsgApp {
		panic("type should be MsgApp")
	}
	return pb.Message{
		From:  m.To,
		To:    m.From,
		Term:  m.Term,
		Type:  pb.MsgAppResp,
		Index: m.Index + uint64(len(m.Entries)),
	}
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
func TestStartAsFollower2AA(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	if r.state != StateFollower {
		t.Errorf("state = %s, want %s", r.state, StateFollower)
	}
}

func TestFollowerUpdateTermFromMessage2AA(t *testing.T) {
	InitLog()
	testUpdateTermFromMessage(t, StateFollower)
}
func TestCandidateUpdateTermFromMessage2AA(t *testing.T) {
	InitLog()
	testUpdateTermFromMessage(t, StateCandidate)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
func testUpdateTermFromMessage(t *testing.T, state StateType) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	}
	r.Step(pb.Message{Type: pb.MsgApp, Term: 2})

	if r.Term != 2 {
		t.Errorf("term = %d, want %d", r.Term, 2)
	}
	if r.state != StateFollower {
		t.Errorf("state = %v, want %v", r.state, StateFollower)
	}
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MessageType_MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
func TestLeaderBcastBeat2AA(t *testing.T) {
	InitLog()
	// heartbeat interval
	hi := 1
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, hi, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages() // clear message
	r.tick()
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat},
		{From: 1, To: 3, Term: 1, Type: pb.MsgHeartbeat},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

func (r *raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)
	return msgs
}

// test follower、candidate start election

func TestFollowerStartElection2AA(t *testing.T) {
	InitLog()
	testNonleaderStartElection(t, StateFollower)
}
func TestCandidateStartNewElection2AA(t *testing.T) {
	InitLog()
	testNonleaderStartElection(t, StateCandidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
func testNonleaderStartElection(t *testing.T, state StateType) {
	// election timeout
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	}

	for i := 1; i < 2*et; i++ {
		r.tick()
	}

	if r.Term != 2 {
		t.Errorf("term = %d, want 2", r.Term)
	}
	if r.state != StateCandidate {
		t.Errorf("state = %s, want %s", r.state, StateCandidate)
	}
	if !r.trk.Votes[r.id] {
		t.Errorf("vote for self = false, want true")
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 2, Type: pb.MsgVote},
		{From: 1, To: 3, Term: 2, Type: pb.MsgVote},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

// TestElectionInOneRoundRPC tests all cases that may happen in
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
func TestElectionInOneRoundRPC2AA(t *testing.T) {
	InitLog()
	tests := []struct {
		size  int
		votes map[uint64]bool
		state StateType
	}{
		// win the election when receiving votes from a majority of the servers
		{1, map[uint64]bool{}, StateLeader},
		{3, map[uint64]bool{2: true, 3: true}, StateLeader},
		{3, map[uint64]bool{2: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, StateLeader},
		{5, map[uint64]bool{2: true, 3: true}, StateLeader},

		// stay in candidate if it does not obtain the majority
		{3, map[uint64]bool{}, StateCandidate},
		{5, map[uint64]bool{2: true}, StateCandidate},
		{5, map[uint64]bool{2: false, 3: false}, StateCandidate},
		{5, map[uint64]bool{}, StateCandidate},
	}
	for i, tt := range tests {
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))

		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		for id, vote := range tt.votes {
			r.Step(pb.Message{From: id, To: 1, Term: r.Term, Type: pb.MsgVoteResp, Reject: !vote})
		}

		if r.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, r.state, tt.state)
		}
		if g := r.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
func TestFollowerVote2AA(t *testing.T) {
	InitLog()
	tests := []struct {
		vote    uint64
		nvote   uint64
		wreject bool
	}{
		{None, 1, false},
		{None, 2, false},
		{1, 1, false},
		{2, 2, false},
		{1, 2, true},
		{2, 1, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
		r.Term = 1
		r.vote = tt.vote

		r.Step(pb.Message{From: tt.nvote, To: 1, Term: 1, Type: pb.MsgVote})

		msgs := r.readMessages()
		wmsgs := []pb.Message{
			{From: 1, To: tt.nvote, Term: 1, Type: pb.MsgVoteResp, Reject: tt.wreject},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %v, want %v", i, msgs, wmsgs)
		}
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
func TestCandidateFallback2AA(t *testing.T) {
	InitLog()
	tests := []pb.Message{
		{From: 2, To: 1, Term: 1, Type: pb.MsgApp},
		{From: 2, To: 1, Term: 2, Type: pb.MsgApp},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
		r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		if r.state != StateCandidate {
			t.Fatalf("unexpected state = %s, want %s", r.state, StateCandidate)
		}

		r.Step(tt)

		if g := r.state; g != StateFollower {
			t.Errorf("#%d: state = %s, want %s", i, g, StateFollower)
		}
		if g := r.Term; g != tt.Term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.Term)
		}
	}
}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

func TestFollowerElectionTimeoutRandomized(t *testing.T) {
	InitLog()
	testNonleaderElectionTimeoutRandomized(t, StateFollower)
}

func TestCandidateElectionTimeoutRandomized(t *testing.T) {
	InitLog()
	testNonleaderElectionTimeoutRandomized(t, StateCandidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
func testNonleaderElectionTimeoutRandomized(t *testing.T, state StateType) {
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	timeouts := make(map[int]bool)
	for round := 0; round < 50*et; round++ {
		switch state {
		case StateFollower:
			r.becomeFollower(r.Term+1, 2)
		case StateCandidate:
			r.becomeCandidate()
		}

		time := 0
		for len(r.readMessages()) == 0 {
			r.tick()
			time++
		}
		timeouts[time] = true
	}

	for d := et + 1; d < 2*et; d++ {
		if !timeouts[d] {
			t.Errorf("timeout in %d ticks should happen", d)
		}
	}
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestLeaderStartReplication(t *testing.T) {
	InitLog()
	s := MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{})
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()

	ents := []pb.Entry{{Data: []byte("some data")}}
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: ents})

	if g := r.raftLog.lastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	if g := r.raftLog.committed; g != li {
		t.Errorf("committed = %d, want %d", g, li)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgApp, Index: li, LogTerm: 1, Entries: wents, Commit: li},
		{From: 1, To: 3, Term: 1, Type: pb.MsgApp, Index: li, LogTerm: 1, Entries: wents, Commit: li},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %+v, want %+v", msgs, wmsgs)
	}
	if g := r.raftLog.unstableEntries(); !reflect.DeepEqual(g, wents) {
		t.Errorf("ents = %+v, want %+v", g, wents)
	}
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
func TestLeaderCommitEntry(t *testing.T) {
	InitLog()
	s := MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{})
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.raftLog.lastIndex()
	r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	if g := r.raftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	if g := r.raftLog.nextCommittedEnts(); !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v, want %+v", g, wents)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	for i, m := range msgs {
		if w := uint64(i + 2); m.To != w {
			t.Errorf("to = %x, want %x", m.To, w)
		}
		if m.Type != pb.MsgApp {
			t.Errorf("type = %v, want %v", m.Type, pb.MsgApp)
		}
		if m.Commit != li+1 {
			t.Errorf("commit = %d, want %d", m.Commit, li+1)
		}
	}
}

//todo  Reference: section 5.4.2
//// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
//// current term are committed by counting replicas.
