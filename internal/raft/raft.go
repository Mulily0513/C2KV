// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/db"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/Mulily0513/C2KV/internal/raft/quorum"
	tracker2 "github.com/Mulily0513/C2KV/internal/raft/tracker"
	"math"
	"math/rand"
	"sync"
	"time"
)

const None uint64 = 0
const InitialTerm uint64 = 0
const noLimit = math.MaxUint64

const (
	defaultMaxInflightMsgs = 256
	defaultMaxSizePerMsg   = 1024 * 1024
)

const (
	StateFollower StateType = iota + 1
	StateCandidate
	StateLeader
)

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateNone",
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[st]
}

type raftOpts struct {
	Id uint64

	peers []uint64

	electionTimeout int

	heartbeatTimeout int

	maxInflightMsgs int

	maxSizePerMsg uint64

	storage db.Storage
}

func (c *raftOpts) validate() error {
	if c.Id == None {
		return errors.New("cannot use none as id")
	}

	if c.heartbeatTimeout <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.electionTimeout <= c.heartbeatTimeout {
		return errors.New("election tick must be greater than heartbeat tick")
	}
	if c.maxInflightMsgs <= 0 {
		c.maxInflightMsgs = defaultMaxInflightMsgs
	}
	if c.maxSizePerMsg == 0 {
		c.maxSizePerMsg = defaultMaxSizePerMsg
	}
	return nil
}

type raft struct {
	id    uint64
	lead  uint64
	Term  uint64
	vote  uint64
	state StateType

	raftLog *raftLog
	// Used for tracking relevant information of nodes.
	trk tracker2.ProgressTracker
	// Messages to be sent to other nodes.
	msgs []*pb.Message
	// Different roles point to different stepFuncs.
	stepFunc stepFunc
	// Different roles point to different tick driving functions.
	tick func()

	electionTimeout  int
	heartbeatTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	electionElapsed           int
	heartbeatElapsed          int

	maxInflightMsgs int
	maxSizePerMsg   uint64
}

func newRaft(opts *raftOpts) (r *raft) {
	if err := opts.validate(); err != nil {
		log.Panicf("verify raft options failed %v", err)
	}

	r = &raft{
		id:               opts.Id,
		lead:             None,
		raftLog:          newRaftLog(opts.storage),
		trk:              tracker2.MakeProgressTracker(opts.peers),
		msgs:             make([]*pb.Message, 0),
		electionTimeout:  opts.electionTimeout,
		heartbeatTimeout: opts.heartbeatTimeout,
		maxInflightMsgs:  opts.maxInflightMsgs,
		maxSizePerMsg:    opts.maxSizePerMsg,
	}
	for _, pr := range r.trk.Progress {
		pr.MaxInflight = r.maxInflightMsgs
	}

	hs := r.raftLog.storage.InitialState()
	if !IsEmptyHardState(hs) {
		r.loadHardState(hs)
	}

	r.becomeFollower(InitialTerm, None)
	return
}

func (r *raft) loadHardState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.vote = state.Vote
}

func (r *raft) softState() SoftState { return SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		log.Panicf("invalid transition [follower -> leader]")
	}
	r.stepFunc = stepLeader
	r.tick = r.tickHeartbeat
	r.reset(r.Term)
	r.lead = r.id
	r.state = StateLeader
	r.trk.Progress[r.id].BecomeReplicate()

	//After becoming a leader, the match of each follower is set to 0, and the next is the next log entry after the last log entry.
	lastIndex := r.raftLog.lastIndex()
	for pr := range r.trk.Progress {
		r.trk.Progress[pr].MaxInflight = r.maxInflightMsgs
		r.trk.Progress[pr].Next = lastIndex + 1
		if pr == r.id {
			// Update the match of the leader to the last index.
			r.trk.Progress[r.id].Match = lastIndex
		} else {
			r.trk.Progress[pr].Match = 0
		}
	}

	//After becoming a leader, an empty log needs to be inserted.
	//emptyEnt := pb.Entry{Data: nil}
	//r.appendEntry(emptyEnt)
	log.Infof("node(id:%x) became leader at term %d", r.id, r.Term)
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.stepFunc = stepFollower
	r.tick = r.tickElection
	r.state = StateFollower
	r.lead = lead
	log.Infof("node(id:%x) became follower at term %d leader is %d", r.id, r.Term, lead)
}

func (r *raft) becomeCandidate() {
	r.reset(r.Term + 1)
	r.stepFunc = stepCandidate
	r.tick = r.tickElection
	r.vote = r.id
	r.state = StateCandidate
	log.Infof("node(id:%x) became candidate at term %d", r.id, r.Term)
}

// tickElection is run by followers and candidates,
func (r *raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		log.Infof("node(id:%x) election timeout, start election", r.id)
		r.electionElapsed = 0
		r.Step(&pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		//todo leader check Quorum
	}

	// Heartbeat timeout. Send heartbeat.
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(&pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

func (r *raft) Step(m *pb.Message) {
	if m.Term != 0 && m.Term < r.Term {
		// Drop stale remote messages to avoid regressing progress with
		// out-of-order responses from old terms.
		if !IsLocalMsg(m.Type) {
			return
		}
	}

	if m.Term > r.Term {
		log.Infof("node(id:%x term: %d) received a %s message with higher term from node(id:%x term: %d)", r.id, r.Term, m.Type, m.From, m.Term)
		if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.Type {
	case pb.MsgHup:
		r.hup()
	case pb.MsgVote:
		canVote := r.vote == m.From || (r.vote == None && r.lead == None)
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.send(&pb.Message{From: r.id, To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type), Reject: false})
			r.electionElapsed = 0
			r.vote = m.From
			log.Infof("node(id:%x) [logterm: %d, index: %d, voteFor: %x] approve %s from node(id:%x) [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
		} else {
			r.send(&pb.Message{From: r.id, To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
			log.Infof("node(id:%x) [logterm: %d, index: %d, voteFor: %x] rejected %s from node(id:%x) [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
		}
	default:
		r.stepFunc(r, m)
	}
}

type stepFunc func(r *raft, m *pb.Message)

func stepLeader(r *raft, m *pb.Message) {
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
	case pb.MsgProp:
		r.handlePropMsg(m)
	case pb.MsgAppResp:
		r.handleAppendResponse(m)
	case pb.MsgHeartbeatResp:
		r.handleHeartbeatResponse(m)
	case pb.MsgUnreachable:
		r.handleMsgUnreachableStatus(m)
	}
	return
}

func stepFollower(r *raft, m *pb.Message) {
	switch m.Type {
	case pb.MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MsgApp:
		r.handleAppendEntries(m)
	}
	return
}

func stepCandidate(r *raft, m *pb.Message) {
	switch m.Type {
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MsgVoteResp:
		r.handleRequestVoteResponse(m)
	}
	return
}

// ------------------- leader behavior -------------------

func (r *raft) bcastHeartbeat() {
	for _, id := range r.trk.Voters.Slice() {
		if r.id != id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *raft) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.trk.Progress[to].Match, r.raftLog.committed)
	m := &pb.Message{
		From:   r.id,
		To:     to,
		Type:   pb.MsgHeartbeat,
		Commit: commit,
		Term:   r.Term,
	}
	r.send(m)
}

func (r *raft) handleHeartbeatResponse(m *pb.Message) {
	pr := r.trk.Progress[m.From]
	if pr == nil {
		log.Errorf("%x no progress available for %x", r.id, m.From)
		return
	}
	pr.RecentActive = true
	pr.ProbeSent = false
	// Drive lagging followers forward on heartbeat responses.
	if pr.Match < r.raftLog.lastIndex() {
		r.sendAppend(m.From)
	}
	return
}

func (r *raft) handleMsgUnreachableStatus(m *pb.Message) {
	pr := r.trk.Progress[m.From]
	if pr == nil {
		log.Errorf("%x no progress available for %x", r.id, m.From)
		return
	}
	if pr.State == tracker2.StateReplicate {
		pr.BecomeProbe()
	}
	log.Infof("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
}

func (r *raft) handleAppendResponse(m *pb.Message) {
	pr := r.trk.Progress[m.From]
	if pr == nil {
		log.Errorf("%x no progress available for %x", r.id, m.From)
		return
	}
	pr.RecentActive = true
	pr.OnAppendResp()
	log.Debugf("get append response from %d, msg:%+v", m.From, *m)
	if m.Reject {
		log.Infof("(node:%x) received MsgAppResp(rejected, hint: (index %d, term %d)) from (node:%x index:%d)", r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
		nextProbeIdx := m.RejectHint
		// Under normal circumstances, the leader's log is longer than that of the follower, and the follower's log is a prefix of the leader's log. In this case, the first probe will reveal the end position of the follower's log (i.e., RejectHint), and subsequent probes will succeed.
		// However, in cases of network partitioning or system overload, there may be a large inconsistent log tail, which can lead to a very time-consuming probing process and may even cause service interruption.
		// To optimize the probing process, a strategy is implemented: if the follower has an uncommitted log tail at the RejectHint index, the leader will determine the location of the next probe based on the LogTerm returned by the follower. If the follower's LogTerm is greater than 0,
		// The leader will check its own log to determine at which indexes the probe will definitely fail because the term of the log entry at these indexes is greater than the follower's LogTerm. In this way, the leader can skip these indexes and only probe those indexes that may succeed.
		// For example, if the leader has:
		//
		//   idx        1 2 3 4 5 6 7 8 9
		//              -----------------
		//   term (L)   1 3 3 3 5 5 5 5 5
		//   term (F)   1 1 1 1 2 2
		//   The follower will return logTerm 2 and index 6. At this time, the leader only needs to take the logTerm of 2 to find the log with a term less than or equal to this term to quickly locate the conflicting log.
		if m.LogTerm > 0 {
			nextProbeIdx = r.raftLog.findConflictIdxByTerm(m.RejectHint, m.LogTerm)
		}
		//If the MaybeDecrTo method is called to backtrack its Next index.
		//If the backtracking fails, it means this is an expired message and no processing is done.
		//If the backtracking is successful and the node is in the StateReplicate state, then the BecomeProbe method is called to change it to the StateProbe state to find the location of the last matching log.
		//When the backtracking is successful, the sendAppend method is also called again for this node to send MsgApp messages.
		if pr.MaybeDecreaseTo(m.Index, nextProbeIdx) {
			log.Infof("%x decreased progress of %x to [%s]", r.id, m.From, pr)
			if pr.State == tracker2.StateReplicate {
				pr.BecomeProbe()
			}
			r.sendAppend(m.From)
		}
		return
	}

	//update peer next、match index
	if pr.MaybeUpdate(m.Index) {
		log.Debugf("update peer progress success, : %+v", *pr)
		if pr.State == tracker2.StateProbe {
			pr.BecomeReplicate()
		}
		if r.maybeLeaderCommit() {
			r.bcastAppend()
		}
	}
	if pr.Match < r.raftLog.lastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *raft) handlePropMsg(m *pb.Message) {
	r.appendEntry(m.Entries...)
	r.bcastAppend()
}

func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.truncateAndAppend(transEnt2Cursor(es))
	r.trk.Progress[r.id].MaybeUpdate(r.raftLog.lastIndex())
}

func (r *raft) bcastAppend() {
	for _, id := range r.trk.Voters.Slice() {
		if r.id != id {
			r.sendAppend(id)
		}
	}
}

func (r *raft) sendAppend(to uint64) {
	pr := r.trk.Progress[to]
	if pr == nil {
		log.Errorf("%x no progress available for %x", r.id, to)
		return
	}
	if pr.IsPaused() {
		return
	}
	log.Debugf("prepare send app msg,peer progress: %+v", *pr)
	m := &pb.Message{To: to}
	lastIndex := r.raftLog.lastIndex()
	if pr.Next < 1 {
		pr.Next = 1
	}
	if pr.Next > lastIndex+1 {
		pr.Next = lastIndex + 1
	}
	prevLogIndex := pr.Next - 1
	prevLogTerm, err := r.raftLog.term(prevLogIndex)
	if err != nil {
		log.Errorf("send append failed get prevlogTerm: %v", err)
		return
	}
	ents, err := r.raftLog.slice(pr.Next, lastIndex+1)
	if err != nil {
		//todo send snapshot?
		log.Errorf("send append failed get entries: %v", err)
		return
	}

	if len(ents) == 0 {
		return
	}
	ents = limitEntriesBySize(ents, r.maxSizePerMsg)
	if len(ents) == 0 {
		return
	}

	m.Type = pb.MsgApp
	m.From = r.id
	m.Term = r.Term
	m.To = to
	m.Index = prevLogIndex
	m.LogTerm = prevLogTerm
	m.Entries = transEnt2Value(ents)
	m.Commit = r.raftLog.committed
	if pr.State == tracker2.StateReplicate {
		// Pipeline replication by moving Next optimistically after enqueueing
		// append messages, following etcd raft's replicate state behavior.
		pr.OptimisticUpdate(m.Entries[len(m.Entries)-1].Index)
	}
	pr.OnAppendSent()
	r.send(m)
	return
}

func limitEntriesBySize(ents []*pb.Entry, maxSize uint64) []*pb.Entry {
	if len(ents) == 0 || maxSize == 0 || maxSize == noLimit {
		return ents
	}
	total := uint64(0)
	limit := len(ents)
	for i := range ents {
		entSize := uint64(ents[i].Size())
		if i > 0 && total+entSize > maxSize {
			limit = i
			break
		}
		total += entSize
	}
	return ents[:limit]
}

func (r *raft) maybeLeaderCommit() bool {
	index := r.trk.Committed()
	return r.raftLog.maybeLeaderCommit(index, r.Term)
}

// ------------------ follower behavior ------------------

func (r *raft) handleHeartbeat(m *pb.Message) {
	r.electionElapsed = 0
	r.lead = m.From
	// A follower may lag behind the leader; never advance committed beyond local lastIndex.
	// Otherwise a stale/overshot heartbeat commit can crash commitTo range checks.
	r.raftLog.commitTo(min(m.Commit, r.raftLog.lastIndex()))
	r.send(&pb.Message{To: m.From, From: r.id, Index: r.raftLog.lastIndex(), Type: pb.MsgHeartbeatResp, Term: r.Term})
}

func (r *raft) handleAppendEntries(m *pb.Message) {
	//If the entry used for log matching is before committed, it means this is an expired message.
	//Therefore, directly return a MsgAppResp message and set the Index field of the message to the value of committed to allow the leader to quickly update the next index of this follower.
	log.Debugf("get append msg: %+v", *m)
	if m.Index < r.raftLog.committed {
		r.send(&pb.Message{To: m.From, From: r.id, Type: pb.MsgAppResp, Index: r.raftLog.committed, Term: r.Term})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, transEnt2Cursor(m.Entries)...); ok {
		r.send(&pb.Message{To: m.From, From: r.id, Type: pb.MsgAppResp, Index: mlastIndex, Term: r.Term})
		return
	}

	log.Infof("node(id:%x logterm: %d, index: %d) rejected MsgApp (node:%x, logterm: %d, index: %d)",
		r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.lastIndex())), r.raftLog.lastIndex(), m.From, m.LogTerm, m.Index)

	hintIndex := min(m.Index, r.raftLog.lastIndex())
	hintIndex = r.raftLog.findConflictIdxByTerm(hintIndex, m.LogTerm)
	hintTerm := r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(hintIndex))
	r.send(&pb.Message{
		To:         m.From,
		From:       r.id,
		Term:       r.Term,
		Type:       pb.MsgAppResp,
		Index:      m.Index,
		Reject:     true,
		RejectHint: hintIndex,
		LogTerm:    hintTerm,
	})
}

// ------------------ candidate behavior ------------------

// Elections can be triggered by heartbeat timeouts or initiated by clients.
func (r *raft) hup() {
	if r.state == StateLeader {
		log.Warnf("%x ignoring MsgHup because already leader", r.id)
		return
	}
	log.Infof("node(id:%x) starting a new election at term %d", r.id, r.Term)
	r.becomeCandidate()
	// If it is a single node, directly become the leader.
	if _, _, res := r.poll(r.id, voteRespMsgType(pb.MsgVote), true); res == quorum.VoteWon {
		r.becomeLeader()
		return
	}
	for _, id := range r.trk.Voters.Slice() {
		if id == r.id {
			continue
		}
		log.Infof("node(id:%x lastterm:%x lastindex:%x) sent %s to %x at term %x", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), pb.MsgVote, id, r.Term)
		r.send(&pb.Message{Term: r.Term, From: r.id, To: id, Type: pb.MsgVote, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm()})
	}
}

func (r *raft) handleRequestVoteResponse(m *pb.Message) {
	gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
	log.Infof("node(id:%x) has received %d %s approval votes and %d vote rejections", r.id, gr, m.Type, rj)
	switch res {
	case quorum.VoteWon:
		r.becomeLeader()
		// todo when become leader,r.bcastAppend()
	case quorum.VoteLost:
		r.becomeFollower(r.Term, None)
	}
}

func (r *raft) sendAllRequestVote() {
	for _, id := range r.trk.Voters.Slice() {
		if id == r.id {
			continue
		}
		log.Infof("node(id:%x logterm: %d, index: %d) sent %s request to %x at term %d", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), pb.MsgVote, id, r.Term)
		r.send(&pb.Message{Term: r.Term, To: id, Type: pb.MsgVote, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm()})
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if id != r.id {
		if v {
			log.Infof("node(id:%x) received %s from node(id:%x) at term %d", r.id, t, id, r.Term)
		} else {
			log.Infof("node(id:%x) received %s rejection from node(id:%x) at term %d", r.id, t, id, r.Term)
		}
	}
	r.trk.RecordVote(id, v)
	return r.trk.TallyVotes()
}

// ------------------ public behavior ------------------------

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.trk.ResetVotes()
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) send(m *pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *raft) advance(rd Ready) {
	if n := len(rd.CommittedEntries); n > 0 {
		applyIdx := rd.CommittedEntries[n-1].Index
		if applyIdx > r.raftLog.applied {
			log.Debugf("appliedTo index: %d", applyIdx)
			r.raftLog.appliedTo(applyIdx)
		} else if applyIdx < r.raftLog.applied {
			// Stale Ready can be observed around leadership changes/conflict repair.
			// Ignore backward applied index and keep monotonic invariant.
			log.Warnf("ignore stale applied index in advance, got=%d current=%d", applyIdx, r.raftLog.applied)
		}
	}

	if n := len(rd.UnstableEntries); n > 0 {
		if newStabled := rd.UnstableEntries[n-1].Index; newStabled > 0 && newStabled > r.raftLog.stabled {
			log.Debugf("stableTo index: %d", newStabled)
			r.raftLog.stableTo(newStabled)
		}
	}
}

func (r *raft) readMessages() []*pb.Message {
	msgs := r.msgs
	r.msgs = make([]*pb.Message, 0)
	return msgs
}

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}
