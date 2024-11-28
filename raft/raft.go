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
	"fmt"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/raft/quorum"
	"github.com/Mulily0513/C2KV/raft/tracker"
	"math"
	"math/rand"
	"sync"
	"time"
)

const None uint64 = 0
const InitialTerm uint64 = 0
const noLimit = math.MaxUint64
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
	trk tracker.ProgressTracker
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
}

func newRaft(opts *raftOpts) (r *raft) {
	if err := opts.validate(); err != nil {
		log.Panicf("verify raft options failed %v", err)
	}

	r = &raft{
		id:               opts.Id,
		lead:             None,
		raftLog:          newRaftLog(opts.storage),
		trk:              tracker.MakeProgressTracker(opts.peers),
		msgs:             make([]*pb.Message, 0),
		electionTimeout:  opts.electionTimeout,
		heartbeatTimeout: opts.heartbeatTimeout,
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
	//todo
	////如果该节点的match index小于leader当前最后一条日志，则为其调用sendAppend方法来复制新日志。
	//if pr.Match < r.raftLog.lastIndex() {
	//	r.sendAppend(m.From)
	//}
	return
}

func (r *raft) handleMsgUnreachableStatus(m *pb.Message) {
	pr := r.trk.Progress[m.From]
	if pr == nil {
		log.Errorf("%x no progress available for %x", r.id, m.From)
		return
	}
	if pr.State == tracker.StateReplicate {
		pr.BecomeProbe()
	}
	log.Infof("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
}

func (r *raft) handleAppendResponse(m *pb.Message) {
	log.Debugf("start handle append response")
	pr := r.trk.Progress[m.From]
	if pr == nil {
		log.Errorf("%x no progress available for %x", r.id, m.From)
		return
	}
	pr.RecentActive = true
	if m.Reject {
		log.Infof("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d", r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
		nextProbeIdx := m.RejectHint
		//在正常情况下，领导者的日志比追随者的日志长，追随者的日志是领导者日志的前缀。在这种情况下，第一次探测（probe）会揭示追随者的日志结束位置（即RejectHint），随后的探测会成功。
		//然而，在网络分区或系统过载的情况下，可能会出现较大的不一致日志尾部，这会导致探测过程非常耗时，甚至可能导致服务中断。
		//为了优化探测过程，实现了一种策略：如果追随者在RejectHint索引处有一个未提交的日志尾部，领导者会根据追随者返回的LogTerm来决定下一次探测的位置。如果追随者的LogTerm大于0，
		//领导者会检查自己的日志，确定在哪些索引处的探测肯定会失败，因为这些索引处的日志项的任期大于追随者的LogTerm。这样，领导者就可以跳过这些索引，只探测那些可能成功的索引。
		// For example, if the leader has:
		//
		//   idx        1 2 3 4 5 6 7 8 9
		//              -----------------
		//   term (L)   1 3 3 3 5 5 5 5 5
		//   term (F)   1 1 1 1 2 2
		//   follower会返回 logTerm 2 index 6,此时leader只需要拿到2这个logTerm去寻找<=该term的日志即可快速定位冲突的日志
		if m.LogTerm > 0 {
			nextProbeIdx = r.raftLog.findConflictIdxByTerm(m.RejectHint, m.LogTerm)
		}
		//调用了MaybeDecrTo方法回退其Next索引。如果回退失败，说明这是一条过期的消息，不做处理；如果回退成功，且该节点为StateReplicate状态，
		//则调用BecomeProbe使其转为StateProbe状态来查找最后一条匹配日志的位置。回退成功时，还会再次为该节点调用sendAppend方法，以为其发送MsgApp消息。
		if pr.MaybeDecreaseTo(m.Index, nextProbeIdx) {
			log.Infof("%x decreased progress of %x to [%s]", r.id, m.From, pr)
			if pr.State == tracker.StateReplicate {
				pr.BecomeProbe()
			}
			r.sendAppend(m.From)
		}
		return
	}

	//update next、match index
	if pr.MaybeUpdate(m.Index) {
		//switch {
		////如果该follower处于StateProbe状态且现在跟上了进度，则将其转为StateReplica状态
		//case pr.State == tracker.StateProbe:
		//	pr.BecomeReplicate()
		//case pr.State == tracker.StateReplicate:
		//}
		if r.maybeCommit() {
			log.Infof("commit update success")
			//r.bcastAppend()
		}
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
	m := &pb.Message{To: to}
	prevLogIndex := pr.Next - 1
	prevLogTerm, errt := r.raftLog.term(prevLogIndex)
	ents, erre := r.raftLog.slice(pr.Next, r.raftLog.lastIndex()+1)
	if errt != nil || erre != nil {
		// todo send snapshot if we failed to get term or entries
	}

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
	r.send(m)
	return
}

func (r *raft) maybeCommit() bool {
	index := r.trk.Committed()
	return r.raftLog.maybeCommit(index, r.Term)
}

// ------------------ follower behavior ------------------

func (r *raft) handleHeartbeat(m *pb.Message) {
	r.electionElapsed = 0
	r.lead = m.From
	r.raftLog.commitTo(m.Commit)
	r.send(&pb.Message{To: m.From, From: r.id, Index: r.raftLog.lastIndex(), Type: pb.MsgHeartbeatResp, Term: r.Term})
}

func (r *raft) handleAppendEntries(m *pb.Message) {
	//如果用于日志匹配的条目在committed之前，说明这是一条过期的消息，因此直接返回MsgAppResp消息，
	//并将消息的Index字段置为committed的值，以让leader快速更新该follower的next index。
	if m.Index < r.raftLog.committed {
		r.send(&pb.Message{To: m.From, From: r.id, Type: pb.MsgAppResp, Index: r.raftLog.committed, Term: r.Term})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, transEnt2Cursor(m.Entries)...); ok {
		r.send(&pb.Message{To: m.From, From: r.id, Type: pb.MsgAppResp, Index: mlastIndex, Term: r.Term})
		return
	}

	log.Infof("node(id:%x logterm: %d, index: %d) rejected MsgApp (logterm: %d, index: %d) from %x",
		r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

	hintIndex := min(m.Index, r.raftLog.lastIndex())
	hintIndex = r.raftLog.findConflictIdxByTerm(hintIndex, m.LogTerm)
	hintTerm, err := r.raftLog.term(hintIndex)
	if err != nil {
		log.Panicf(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
	}
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
		r.bcastAppend()
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
	// todo there have two choice to update committed index or stable index
	// one for ready ,another for storage

	if n := len(rd.CommittedEntries); n > 0 {
		log.Debugf("appliedTo index: %d", rd.CommittedEntries[n-1].Index)
		r.raftLog.appliedTo(rd.CommittedEntries[n-1].Index)
	}

	//todo
	//if n := len(rd.UnstableEntries);n>0{
	//	if newStabled := rd.UnstableEntries[n-1].Index; newStabled > 0 && newStabled > r.raftLog.stabled {
	//		r.raftLog.stableTo(newStabled)
	//	}
	//}

	if newStabled := r.raftLog.storage.StableIndex(); newStabled > 0 && newStabled > r.raftLog.stabled {
		r.raftLog.stableTo(newStabled)
		log.Debugf("stableTo index: %d", newStabled)
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
