package raft

import (
	"github.com/ColdToo/Cold2DB/pb"
	"testing"
)

// TestNodeStep ensures that node.Step sends msgProp to propc chan
// and other kinds of messages to recvc chan.
func TestNodeStep(t *testing.T) {
	for i, msgn := range pb.MessageType_name {
		n := &raftNode{
			propC:    make(chan *pb.Message, 1),
			receiveC: make(chan *pb.Message, 1),
		}
		msgt := pb.MessageType(i)
		n.Step(&pb.Message{Type: msgt})
		// Proposal goes to proc chan. Others go to recvc chan.
		if msgt == pb.MsgProp {
			select {
			case <-n.propC:
			default:
				t.Errorf("%d: cannot receive %s on propc chan", msgt, msgn)
			}
		} else {
			if IsLocalMsg(msgt) {
				select {
				case <-n.receiveC:
					t.Errorf("%d: step should ignore %s", msgt, msgn)
				default:
				}
			} else {
				select {
				case <-n.receiveC:
				default:
					t.Errorf("%d: cannot receive %s on recvc chan", msgt, msgn)
				}
			}
		}
	}
}
