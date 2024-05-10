package raft

import (
	"fmt"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/db/mocks"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/golang/mock/gomock"
	"reflect"
	"sort"
	"testing"
)

type messageSlice []*pb.Message

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
	return newRaft(RaftOpts(id, peers, election, heartbeat, storage))
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

func commitNoopEntry(r *raft) {
	if r.state != StateLeader {
		panic("it should only be used when it is the leader")
	}
	r.bcastAppend()

	// simulate the response of MsgApp all accept
	msgs := r.readMessages()
	for _, m := range msgs {
		if m.Type != pb.MsgApp || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}

	// simulate advance after ready
	r.readMessages()
	r.raftLog.appliedTo(r.raftLog.committed)
	r.raftLog.stableTo(r.raftLog.lastIndex())
}

func acceptAndReply(m *pb.Message) *pb.Message {
	if m.Type != pb.MsgApp {
		panic("type should be MsgApp")
	}
	return &pb.Message{
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
	r.Step(&pb.Message{Type: pb.MsgApp, Term: 2})

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
	heartbeat := 1
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, heartbeat, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages() // clear message
	r.tick()
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []*pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat},
		{From: 1, To: 3, Term: 1, Type: pb.MsgHeartbeat},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
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
	wmsgs := []*pb.Message{
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

		r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgHup})
		for id, vote := range tt.votes {
			r.Step(&pb.Message{From: id, To: 1, Term: r.Term, Type: pb.MsgVoteResp, Reject: !vote})
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

		r.Step(&pb.Message{From: tt.nvote, To: 1, Term: 1, Type: pb.MsgVote})

		msgs := r.readMessages()
		wmsgs := []*pb.Message{
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
	tests := []*pb.Message{
		{From: 2, To: 1, Term: 1, Type: pb.MsgApp},
		{From: 2, To: 1, Term: 2, Type: pb.MsgApp},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
		r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgHup})
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
	li := r.raftLog.lastIndex()

	ents := []pb.Entry{{Data: []byte("some data")}}
	r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: ents})
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wents := []pb.Entry{{Index: li, Term: 1, Data: nil}, {Index: li + 1, Term: 1, Data: []byte("some data")}}
	wmsgs := []*pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgApp, Index: 0, LogTerm: 0, Entries: wents, Commit: 0},
		{From: 1, To: 3, Term: 1, Type: pb.MsgApp, Index: 0, LogTerm: 0, Entries: wents, Commit: 0},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %+v, want %+v", msgs, wmsgs)
	}
	if g := r.raftLog.unstableEntries(); !reflect.DeepEqual(g, transEnt2Cursor(wents)) {
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
	commitNoopEntry(r)
	li := r.raftLog.lastIndex()
	r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	if g := r.raftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []*pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
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

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
func TestLeaderAcknowledgeCommit(t *testing.T) {
	InitLog()
	tests := []struct {
		size      int
		acceptors map[uint64]bool
		wack      bool
	}{
		{3, nil, false},
		{3, map[uint64]bool{2: true}, true},
		{3, map[uint64]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[uint64]bool{2: true}, false},
		{5, map[uint64]bool{2: true, 3: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, true},
	}

	for i, tt := range tests {
		s := MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{})
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, s)
		r.becomeCandidate()
		r.becomeLeader()
		commitNoopEntry(r)
		li := r.raftLog.lastIndex()
		r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			if tt.acceptors[m.To] {
				r.Step(acceptAndReply(m))
			}
		}

		if g := r.raftLog.committed > li; g != tt.wack {
			t.Errorf("#%d: ack commit = %v, want %v", i, g, tt.wack)
		}
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestLeaderCommitPrecedingEntries(t *testing.T) {
	InitLog()
	tests := [][]*pb.Entry{
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		s := MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{})
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
		r.raftLog.truncateAndAppend(tt)
		r.Term = 2
		r.becomeCandidate()
		r.becomeLeader()
		r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			r.Step(acceptAndReply(m))
		}

		li := uint64(len(tt))
		wents := append(tt, &pb.Entry{Term: 3, Index: li + 1}, &pb.Entry{Term: 3, Index: li + 2, Data: []byte("some data")})
		if g := r.raftLog.nextCommittedEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v, want %+v", i, g, wents)
		}
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
func TestFollowerCommitEntry(t *testing.T) {
	InitLog()
	tests := []struct {
		ents   []*pb.Entry
		commit uint64
	}{
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
			},
			1,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			2,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data2")},
				{Term: 1, Index: 2, Data: []byte("some data")},
			},
			2,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		s := MockStorage(t, 0, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{})
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
		r.becomeFollower(1, 2)

		r.Step(&pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 1, Entries: transEnt2Value(tt.ents), Commit: tt.commit})

		if g := r.raftLog.committed; g != tt.commit {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.commit)
		}
		wents := tt.ents[:int(tt.commit)]
		if g := r.raftLog.nextCommittedEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: nextEnts = %v, want %v", i, g, wents)
		}
	}
}

// TestFollowerCheckMsgApp tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
func TestFollowerCheckMsgApp(t *testing.T) {
	InitLog()
	tests := []struct {
		term        uint64
		index       uint64
		windex      uint64
		wreject     bool
		wrejectHint uint64
		s           db.Storage
	}{
		// match with committed entries
		{0, 0, 1, false, 0, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{})},
		{1, 1, 1, false, 0, MockStorage(t, 0, 0, 1, 1, 1, pb.HardState{}, pb.ConfState{})},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, tt.s)
		r.raftLog.committed = 1
		r.becomeFollower(2, 2)
		r.Step(&pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, LogTerm: tt.term, Index: tt.index})

		msgs := r.readMessages()
		wmsgs := []*pb.Message{
			{From: 1, To: 2, Type: pb.MsgAppResp, Term: 2, Index: tt.windex, Reject: tt.wreject, RejectHint: tt.wrejectHint},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %+v, want %+v", i, msgs, wmsgs)
		}
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
func TestFollowerAppendEntries(t *testing.T) {
	InitLog()
	tests := []struct {
		name        string
		index, term uint64
		ents        []*pb.Entry
		wents       []*pb.Entry
	}{
		{"1",
			2, 2,
			[]*pb.Entry{{Term: 3, Index: 3}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
		},
		{"2",
			1, 1,
			[]*pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{})
			r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
			r.raftLog.truncateAndAppend([]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
			r.becomeFollower(2, 2)
			r.Step(&pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: 2, LogTerm: tt.term, Index: tt.index, Entries: transEnt2Value(tt.ents)})

			if g := r.raftLog.unstableEntries(); !reflect.DeepEqual(g, tt.wents) {
				t.Errorf("#%d: unstableEnts = %+v, want %+v", i, g, tt.wents)
			}
		})
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
func TestVoteRequest(t *testing.T) {
	InitLog()
	tests := []struct {
		ents  []pb.Entry
		wterm uint64
	}{
		{[]pb.Entry{{Term: 1, Index: 1}}, 2},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 3},
	}
	for j, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
		r.Step(&pb.Message{From: 2, To: 1, Type: pb.MsgApp, Term: tt.wterm - 1, LogTerm: 0, Index: 0, Entries: tt.ents})
		r.readMessages()

		for i := 1; i < r.electionTimeout*2; i++ {
			r.tickElection()
		}

		msgs := r.readMessages()
		sort.Sort(messageSlice(msgs))
		if len(msgs) != 2 {
			t.Fatalf("#%d: len(msg) = %d, want %d", j, len(msgs), 2)
		}
		for i, m := range msgs {
			if m.Type != pb.MsgVote {
				t.Errorf("#%d: msgType = %d, want %d", i, m.Type, pb.MsgVote)
			}
			if m.To != uint64(i+2) {
				t.Errorf("#%d: to = %d, want %d", i, m.To, i+2)
			}
			if m.Term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, m.Term, tt.wterm)
			}
			windex, wlogterm := tt.ents[len(tt.ents)-1].Index, tt.ents[len(tt.ents)-1].Term
			if m.Index != windex {
				t.Errorf("#%d: index = %d, want %d", i, m.Index, windex)
			}
			if m.LogTerm != wlogterm {
				t.Errorf("#%d: logterm = %d, want %d", i, m.LogTerm, wlogterm)
			}
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
func TestVoter(t *testing.T) {
	InitLog()
	tests := []struct {
		ents    []pb.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 1, true},
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 2, true},
		{[]pb.Entry{{Term: 2, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
		r.raftLog.truncateAndAppend(transEnt2Cursor(tt.ents))

		r.Step(&pb.Message{From: 2, To: 1, Type: pb.MsgVote, Term: 3, LogTerm: tt.logterm, Index: tt.index})

		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msg) = %d, want %d", i, len(msgs), 1)
		}
		m := msgs[0]
		if m.Type != pb.MsgVoteResp {
			t.Errorf("#%d: msgType = %d, want %d", i, m.Type, pb.MsgVoteResp)
		}
		if m.Reject != tt.wreject {
			t.Errorf("#%d: reject = %t, want %t", i, m.Reject, tt.wreject)
		}
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
func TestLeaderOnlyCommitsLogFromCurrentTerm(t *testing.T) {
	InitLog()
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockStorage(t, 0, 0, 0, 0, 0, pb.HardState{}, pb.ConfState{}))
		r.raftLog.truncateAndAppend(transEnt2Cursor(ents))
		r.Term = 2
		// become leader at term 3
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		r.Step(&pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{}}})

		r.Step(&pb.Message{From: 2, To: 1, Type: pb.MsgAppResp, Term: r.Term, Index: tt.index})
		if r.raftLog.committed != tt.wcommit {
			t.Errorf("#%d: commit = %d, want %d", i, r.raftLog.committed, tt.wcommit)
		}
	}
}
