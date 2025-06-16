// Copyright 2019 The etcd Authors
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

package tracker

import (
	"fmt"
	"github.com/Mulily0513/C2KV/src/pb"
	quorum2 "github.com/Mulily0513/C2KV/src/raft/quorum"
)

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
type ProgressTracker struct {
	Voters quorum2.MajorityConfig

	Progress ProgressMap

	Votes map[uint64]bool
}

func MakeProgressTracker(peers []uint64) ProgressTracker {
	prs := make(map[uint64]*Progress)
	for _, pr := range peers {
		prs[pr] = &Progress{
			Next:  0,
			Match: 0,
		}
	}

	voters := make(quorum2.MajorityConfig)
	for _, peer := range peers {
		voters[peer] = struct{}{}
	}

	p := ProgressTracker{
		Voters:   voters,
		Votes:    map[uint64]bool{},
		Progress: prs,
	}
	return p
}

func (p *ProgressTracker) ConfState() pb.ConfState {
	return pb.ConfState{Voters: p.Voters.Slice()}
}

func (p *ProgressTracker) IsSingleton() bool {
	return len(p.Voters) == 1
}

type matchAckIndexer map[uint64]*Progress

func (l matchAckIndexer) AckedIndex(id uint64) (quorum2.Index, bool) {
	pr, ok := l[id]
	if !ok {
		return 0, false
	}
	return quorum2.Index(pr.Match), true
}

// Committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged.
func (p *ProgressTracker) Committed() uint64 {
	return uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
}

func (p *ProgressTracker) VoterNodes() []uint64 {
	return p.Voters.Slice()
}

func (p *ProgressTracker) VoterNodesStr() (nodesStrs []string) {
	for _, n := range p.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}
	return
}

func (p *ProgressTracker) ResetVotes() {
	p.Votes = map[uint64]bool{}
}

func (p *ProgressTracker) RecordVote(id uint64, v bool) {
	_, ok := p.Votes[id]
	if !ok {
		p.Votes[id] = v
	}
}

// TallyVotes returns the number of granted and rejected Votes, and whether the election outcome is known.
func (p *ProgressTracker) TallyVotes() (granted int, rejected int, _ quorum2.VoteResult) {
	for id, _ := range p.Progress {
		v, voted := p.Votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := p.Voters.VoteResult(p.Votes)
	return granted, rejected, result
}
