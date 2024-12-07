package raft

import (
	"errors"
	"github.com/Mulily0513/C2KV/code"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
)

//  log structure
//  ......persist................applied|first.................committed.................stabled....................last
//	--------|--------mem-table----------|--------------------storage slice------------------|-----raft log slice------|
//	--vlog--|--------------------------wal--------------------------------------------------|

type raftLog struct {
	// Index of the last log entry already applied to the memtable.
	applied uint64

	committed uint64

	// Equal to the last index of stable storage.
	stabled uint64

	// This offset (u.offset) represents the position of the first entry in the current unpersisted log within the entire log.
	// For example, if u.offset is 10, then the position of the first entry in the unpersisted log is the 10th position.
	// When raftLog is created, the offset is set to the last index of storage plus 1.
	offset uint64

	unstableEnts []*pb.Entry

	storage db.Storage
}

func newRaftLog(storage db.Storage) (r *raftLog) {
	r = &raftLog{storage: storage}
	r.unstableEnts = make([]*pb.Entry, 0)
	r.stabled = storage.StableIndex()
	r.offset = r.stabled + 1
	r.applied = storage.AppliedIndex()
	return
}

func (l *raftLog) firstIndex() uint64 {
	firstIndex := l.storage.FirstIndex()
	if firstIndex == 0 {
		if len(l.unstableEnts) != 0 {
			return l.unstableEnts[0].Index
		}
		return 0
	} else {
		return firstIndex
	}
}

func (l *raftLog) lastIndex() uint64 {
	if length := len(l.unstableEnts); length != 0 {
		return l.offset + uint64(length) - 1
	}
	return l.stabled
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		log.Errorf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

func (l *raftLog) term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}

	if i == l.storage.AppliedIndex() {
		return l.storage.AppliedTerm(), nil
	}

	log.Debugf("request index:%d,appliedIndex:%d,lastIndex:%d", i, l.storage.AppliedIndex(), l.lastIndex())
	if i < l.storage.AppliedIndex() || i > l.lastIndex() {
		return 0, code.ErrUnavailable
	}

	if i > l.stabled {
		return l.unstableEnts[i-l.offset].Term, nil
	}

	t, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return t, nil
}

// Entries gets the log slice after the specified index.
func (l *raftLog) entries(i uint64) (ents []*pb.Entry, err error) {
	if i > l.lastIndex() {
		return nil, code.ErrUnavailable
	}
	return l.slice(i, l.lastIndex()+1)
}

// slice returns a slice of log entries from lo through hi-1, [lo,hi)
func (l *raftLog) slice(lo, hi uint64) ([]*pb.Entry, error) {
	if lo == hi {
		return nil, nil
	}

	if err := l.mustCheckOutOfBounds(lo, hi); err != nil {
		return nil, err
	}

	// If lo is less than offset, obtain this part of the log from storage.
	var ents []*pb.Entry
	if lo < l.offset {
		stableEnts, err := l.storage.Entries(lo, min(hi, l.offset))
		if errors.Is(err, code.ErrCompacted) {
			return nil, err
		}

		if uint64(len(stableEnts)) < min(hi, l.offset)-lo {
			return stableEnts, nil
		}

		ents = stableEnts
	}

	//If hi is greater than offset, obtain this part of the log from unstableEnts.
	if hi > l.offset {
		unstableEnts := l.unstableEnts[max(lo, l.offset)-l.offset : hi-l.offset]
		// If ents is not empty, merge unstableEnts with ents.
		if len(ents) > 0 {
			combined := make([]*pb.Entry, len(ents)+len(unstableEnts))
			n := copy(combined, ents)
			copy(combined[n:], unstableEnts)
			ents = combined
		} else {
			ents = unstableEnts
		}
	}
	return ents, nil
}

// lo < hi? (Is lo less than hi? The slice obtained is a log slice in the left-closed and right-open interval [lo, hi).)
// lo < firstIndex? (If lo is less than firstIndex, part of the logs in this range have been compacted and cannot be obtained.)
// hi > lastIndex? (If hi is greater than lastIndex, part of the logs in this range have not been appended to the current node's log and cannot be obtained.)
// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	li := l.lastIndex()
	if lo < fi {
		return code.ErrCompacted
	}
	if hi > li+1 {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, li)
	}
	return nil
}

// for ready

func (l *raftLog) unstableEntries() []*pb.Entry {
	if len(l.unstableEnts) == 0 {
		return nil
	}
	return l.unstableEnts
}

func (l *raftLog) nextCommittedEnts() (ents []*pb.Entry) {
	if l.committed > l.applied {
		ents, err := l.slice(l.applied+1, l.committed+1)
		if err != nil {
			log.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

func (l *raftLog) hasNextCommittedEnts() bool {
	return l.committed > l.applied
}

// "truncate append" and "maybeAppend" are both methods for writing logs to raftLog.
// The difference between them is that "truncate append" does not check whether the given log slice conflicts with the existing log.
// This method is called by the leader node. The follower will call the "maybeAppend" method.
// This method will check whether the given log slice conflicts with the existing log. This method is called by the follower node.
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	// First determine if it matches the last log.
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		// Judge whether there are conflicting logs in the logs to be added.
		ci := l.findConflict(ents)
		switch {
		//It means that there is neither conflict nor new log. Proceed to the next step directly.
		case ci == 0:
			//Check whether the starting point of the conflict log is at or before the committed index position. If so, this violates the Log Matching property of the Raft algorithm.
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
			//If the return value is greater than committed, it could either be that the conflict occurred after committed or there is a new log.
			//However, the handling method for both cases is the same, that is, start from the conflict or new log location and overwrite or append the logs to the current log.
		default:
			offset := index + 1
			l.truncateAndAppend(ents[ci-offset:])
		}
		//The value of lastnewi may be less than committed. This is due to the leader not sending a complete entry.
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

// If the given log conflicts with the existing log in terms of index and term, it will return the index of the first conflicting log entry.
// If there is no conflict and all entries of the given log are already in the existing log, return 0.
// If there is no conflict and the given log contains new logs not in the existing log, return the index of the first new log.
func (l *raftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]", ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *raftLog) truncateAndAppend(ents []*pb.Entry) {
	after := ents[0].Index
	// Check if the given start index of the log is before the committed index position.
	// If it is, this violates the Log Matching property of the Raft algorithm.
	if after <= l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	switch {
	// after is the next index in the unstable Entries directly append
	case after == l.offset+uint64(len(l.unstableEnts)):
		l.unstableEnts = append(l.unstableEnts, ents...)
	case after < l.offset:
		log.Infof("replace the unstable entries from index %d", after)
		if err := l.storage.Truncate(after); err != nil {
			log.Panicf("failed to truncate the stable entries before index %d,err:%v", after, err)
		}
		l.offset = after
		l.unstableEnts = ents
	case after >= l.offset:
		log.Infof("truncate the unstable entries before index %d", after)
		l.unstableEnts = append([]*pb.Entry{}, l.unstableEnts[:after-l.offset]...)
		l.unstableEnts = append(l.unstableEnts, ents...)
	default:
		log.Infof("unexpected truncateAndAppend case")
	}
}

func (l *raftLog) findConflictIdxByTerm(index uint64, term uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		log.Warnf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm", index, li)
		return index
	}
	for {
		logTerm, err := l.term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == code.ErrCompacted {
		return 0
	}
	if err == code.ErrUnavailable {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *raftLog) maybeLeaderCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		log.Debugf("leader commit index to %d", maxIndex)
		return true
	}
	return false
}

func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if i > l.committed || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *raftLog) stableTo(i uint64) {
	if i >= l.offset {
		l.unstableEnts = l.unstableEnts[i+1-l.offset:]
		l.offset = i + 1
		l.stabled = i
		l.shrinkEntriesArray()
	}
}

func (l *raftLog) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(l.unstableEnts) == 0 {
		l.unstableEnts = nil
	} else if len(l.unstableEnts)*lenMultiple < cap(l.unstableEnts) {
		newEntries := make([]*pb.Entry, len(l.unstableEnts))
		copy(newEntries, l.unstableEnts)
		l.unstableEnts = newEntries
	}
}
