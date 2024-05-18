package raft

import (
	"bytes"
	"fmt"
	"github.com/Mulily0513/C2KV/pb"
	"strings"
)

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, pb.HardState{})
}

func IsEmptyConfState(cs pb.ConfState) bool {
	return cs.Voters == nil
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MsgHup || msgt == pb.MsgBeat
}

func IsResponseMsg(msgt pb.MessageType) bool {
	return msgt == pb.MsgAppResp || msgt == pb.MsgVoteResp || msgt == pb.MsgHeartbeatResp
}

// voteResponseType maps vote and prevote message types to their corresponding responses.
func voteRespMsgType(msgt pb.MessageType) pb.MessageType {
	switch msgt {
	case pb.MsgVote:
		return pb.MsgVoteResp
	case pb.MsgPreVote:
		return pb.MsgPreVoteResp
	default:
		panic(fmt.Sprintf("not a vote message: %s", msgt))
	}
}

func DescribeHardState(hs pb.HardState) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "Term:%d", hs.Term)
	if hs.Vote != 0 {
		fmt.Fprintf(&buf, " Vote:%d", hs.Vote)
	}
	fmt.Fprintf(&buf, " Commit:%d", hs.Commit)
	return buf.String()
}

func DescribeSoftState(ss SoftState) string {
	return fmt.Sprintf("Lead:%d State:%s", ss.Lead, ss.RaftState)
}

func DescribeConfState(state pb.ConfState) string {
	return fmt.Sprintf(
		"Voters:%v VotersOutgoing:%v Learners:%v LearnersNext:%v AutoLeave:%v",
		state.Voters, state.VotersOutgoing, state.Learners, state.LearnersNext, state.AutoLeave,
	)
}

func DescribeSnapshot(snap pb.Snapshot) string {
	m := snap.Metadata
	return fmt.Sprintf("Index:%d Term:%d ConfState:%s", m.Index, m.Term, DescribeConfState(m.ConfState))
}

func DescribeReady(rd Ready, f EntryFormatter) string {
	var buf strings.Builder
	//if rd.SoftState != nil {
	//	fmt.Fprint(&buf, DescribeSoftState(*rd.SoftState))
	//	buf.WriteByte('\n')
	//}
	//if !IsEmptyHardState(rd.HardState) {
	//	fmt.Fprintf(&buf, "HardState %s", DescribeHardState(rd.HardState))
	//	buf.WriteByte('\n')
	//}
	//if len(rd.UnstableEntries) > 0 {
	//	buf.WriteString("Entries:\n")
	//	fmt.Fprint(&buf, DescribeEntries(rd.UnstableEntries, f))
	//}
	//if len(rd.CommittedEntries) > 0 {
	//	buf.WriteString("CommittedEntries:\n")
	//	fmt.Fprint(&buf, DescribeEntries(rd.CommittedEntries, f))
	//}
	if len(rd.Messages) > 0 {
		buf.WriteString("Messages:\n")
		for _, msg := range rd.Messages {
			fmt.Fprint(&buf, DescribeMessage(msg, f))
			buf.WriteByte('\n')
		}
	}
	return "<empty Ready>"
}

// EntryFormatter can be implemented by the application to provide human-readable formatting
// of entry data. Nil is a valid EntryFormatter and will use a default format.
type EntryFormatter func([]byte) string

// DescribeMessage returns a concise human-readable description of a
// Message for debugging.
func DescribeMessage(m *pb.Message, f EntryFormatter) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%x->%x %v Term:%d Log:%d/%d", m.From, m.To, m.Type, m.Term, m.LogTerm, m.Index)
	if m.Reject {
		fmt.Fprintf(&buf, " Rejected (Hint: %d)", m.RejectHint)
	}
	if m.Commit != 0 {
		fmt.Fprintf(&buf, " Commit:%d", m.Commit)
	}
	if len(m.Entries) > 0 {
		fmt.Fprintf(&buf, " Entries:[")
		for i, e := range m.Entries {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(DescribeEntry(e, f))
		}
		fmt.Fprintf(&buf, "]")
	}
	if !IsEmptySnap(m.Snapshot) {
		fmt.Fprintf(&buf, " Snapshot: %s", DescribeSnapshot(m.Snapshot))
	}
	return buf.String()
}

// PayloadSize is the size of the payload of this Entry. Notably, it does not
// depend on its Index or Term.
func PayloadSize(e pb.Entry) int {
	return len(e.Data)
}

// DescribeEntry returns a concise human-readable description of an
// Entry for debugging.
func DescribeEntry(e pb.Entry, f EntryFormatter) string {
	if f == nil {
		f = func(data []byte) string { return fmt.Sprintf("%q", data) }
	}

	//formatConfChange := func(cc raftpb.ConfChangeI) string {
	//	// TODO(tbg): give the EntryFormatter a type argument so that it gets
	//	// a chance to expose the Context.
	//	return raftpb.ConfChangesToString(cc.AsV2().Changes)
	//}

	var formatted string
	switch e.Type {
	case pb.EntryNormal:
		formatted = f(e.Data)
	case pb.EntryConfChange:
		var cc pb.ConfChange
		if err := cc.Unmarshal(e.Data); err != nil {
			formatted = err.Error()
		} else {
			//formatted = formatConfChange(cc)
		}
	case pb.EntryConfChangeV2:
		var cc pb.ConfChangeV2
		if err := cc.Unmarshal(e.Data); err != nil {
			formatted = err.Error()
		} else {
			//formatted = formatConfChange(cc)
		}
	}
	if formatted != "" {
		formatted = " " + formatted
	}
	return fmt.Sprintf("%d/%d %s%s", e.Term, e.Index, e.Type, formatted)
}

// DescribeEntries calls DescribeEntry for each Entry, adding a newline to
// each.
func DescribeEntries(ents []pb.Entry, f EntryFormatter) string {
	var buf bytes.Buffer
	for _, e := range ents {
		_, _ = buf.WriteString(DescribeEntry(e, f) + "\n")
	}
	return buf.String()
}

func limitSize(ents []pb.Entry, maxSize uint64) []pb.Entry {
	if len(ents) == 0 {
		return ents
	}
	if maxSize == noLimit {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}

func transEnt2Cursor(ent []pb.Entry) []*pb.Entry {
	trans := make([]*pb.Entry, len(ent))
	for i := range ent {
		trans[i] = &ent[i]
	}
	return trans
}

func transEnt2Value(ent []*pb.Entry) []pb.Entry {
	trans := make([]pb.Entry, len(ent))
	for i := range ent {
		trans[i] = *ent[i]
	}
	return trans
}
