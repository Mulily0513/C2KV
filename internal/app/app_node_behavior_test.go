package app

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	dbmocks "github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/Mulily0513/C2KV/internal/raft"
	"github.com/Mulily0513/C2KV/internal/transport/types"
	"github.com/golang/mock/gomock"
)

type appNodeTestRaftNode struct {
	status raft.Status
	stepCh chan *pb.Message
	stopCh chan struct{}
}

func newAppNodeTestRaftNode() *appNodeTestRaftNode {
	return &appNodeTestRaftNode{
		stepCh: make(chan *pb.Message, 8),
		stopCh: make(chan struct{}, 1),
	}
}

func (n *appNodeTestRaftNode) Tick() {}

func (n *appNodeTestRaftNode) Propose(_ []byte) {}

func (n *appNodeTestRaftNode) ReportUnreachable(_ uint64) {}

func (n *appNodeTestRaftNode) Status() raft.Status { return n.status }

func (n *appNodeTestRaftNode) Step(msg *pb.Message) { n.stepCh <- msg }

func (n *appNodeTestRaftNode) Ready() <-chan raft.Ready { return nil }

func (n *appNodeTestRaftNode) Advance() {}

func (n *appNodeTestRaftNode) Stop() { n.stopCh <- struct{}{} }

type appNodeTestTransport struct {
	stopCh chan struct{}
}

func (t *appNodeTestTransport) ListenPeer(_ net.Listener) {}

func (t *appNodeTestTransport) Send(_ []*pb.Message) {}

func (t *appNodeTestTransport) AddPeer(_ types.ID, _ string) {}

func (t *appNodeTestTransport) Stop() { t.stopCh <- struct{}{} }

func makeCommittedEntry(index uint64, key, value string, applyID uint64) *pb.Entry {
	applySig := make([]byte, marshal.ApplySigSize)
	binary.LittleEndian.PutUint64(applySig, applyID)
	kv := &marshal.KV{
		ApplySig: applySig,
		Key:      []byte(key),
		Data: &marshal.Data{
			Index:     0,
			TimeStamp: time.Now().Unix(),
			Type:      marshal.TypeInsert,
			Value:     []byte(value),
		},
	}
	return &pb.Entry{
		Term:  index,
		Index: index,
		Type:  pb.EntryNormal,
		Data:  marshal.EncodeKV(kv),
	}
}

func TestAppNodeApplyCommittedEntsAppliesAndSignals(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := dbmocks.NewMockStorage(ctrl)
	const applyID = uint64(42)
	monitor := newProposalMonitor()
	signal := monitor.acquireSignal()
	defer monitor.releaseSignal(signal)
	monitor.put(applyID, signal)

	entry := makeCommittedEntry(42, "k1", "v1", applyID)
	ignored := &pb.Entry{Type: pb.EntryConfChange, Index: 43}
	storage.EXPECT().Apply(gomock.Any()).DoAndReturn(func(kvs []*marshal.KV) error {
		if len(kvs) != 1 {
			t.Fatalf("expected one normal entry to apply, got %d", len(kvs))
		}
		if !bytes.Equal(kvs[0].Key, []byte("k1")) {
			t.Fatalf("unexpected applied key: %q", string(kvs[0].Key))
		}
		var decoded *marshal.Data
		if kvs[0].Data != nil {
			decoded = kvs[0].Data
		} else {
			decoded = marshal.DecodeData(kvs[0].DataBytes)
		}
		if !bytes.Equal(decoded.Value, []byte("v1")) {
			t.Fatalf("unexpected applied value: %q", string(decoded.Value))
		}
		if decoded.Index != entry.Index {
			t.Fatalf("expected applied data index=%d, got %d", entry.Index, decoded.Index)
		}
		return nil
	}).Times(1)

	an := &AppNode{
		kvStorage: storage,
		monitor:   monitor,
	}
	an.applyCommittedEnts([]*pb.Entry{entry, ignored})

	select {
	case <-signal:
	default:
		t.Fatal("apply signal was not delivered")
	}

	if got := monitor.len(); got != 0 {
		t.Fatal("apply signal should be removed from monitor map")
	}
}

func TestAppNodeServePropCAndConfCForwardsProposal(t *testing.T) {
	node := newAppNodeTestRaftNode()
	an := &AppNode{
		proposeC: make(chan []byte, 2),
		raftNode: node,
	}

	done := make(chan struct{})
	go func() {
		an.servePropCAndConfC()
		close(done)
	}()

	want := []byte("proposal-1")
	want2 := []byte("proposal-2")
	an.proposeC <- want
	an.proposeC <- want2
	close(an.proposeC)

	select {
	case msg := <-node.stepCh:
		if msg.Type != pb.MsgProp {
			t.Fatalf("expected proposal message, got=%s", msg.Type)
		}
		if len(msg.Entries) != 2 {
			t.Fatalf("expected batched entries=2, got=%d", len(msg.Entries))
		}
		if !bytes.Equal(msg.Entries[0].Data, want) {
			t.Fatalf("proposal #1 mismatch, want=%q got=%q", string(want), string(msg.Entries[0].Data))
		}
		if !bytes.Equal(msg.Entries[1].Data, want2) {
			t.Fatalf("proposal #2 mismatch, want=%q got=%q", string(want2), string(msg.Entries[1].Data))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for forwarded proposal")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("servePropCAndConfC did not exit after propose channel set to nil")
	}
}

func TestAppNodeProcessCallsRaftStep(t *testing.T) {
	node := newAppNodeTestRaftNode()
	an := &AppNode{raftNode: node}
	msg := &pb.Message{Type: pb.MsgApp, To: 2, From: 1}

	an.Process(msg)

	select {
	case got := <-node.stepCh:
		if got != msg {
			t.Fatalf("step message mismatch, want=%p got=%p", msg, got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for raft Step")
	}
}

func TestAppNodeCloseStopsAndClosesChannels(t *testing.T) {
	node := newAppNodeTestRaftNode()
	tr := &appNodeTestTransport{stopCh: make(chan struct{}, 1)}
	an := &AppNode{
		raftNode:       node,
		transport:      tr,
		proposeC:       make(chan []byte),
		confChangeC:    make(chan pb.ConfChange),
		kvServiceStopC: make(chan struct{}),
	}

	an.Close()

	select {
	case <-tr.stopCh:
	default:
		t.Fatal("transport stop was not called")
	}
	select {
	case <-node.stopCh:
	default:
		t.Fatal("raft node stop was not called")
	}

	select {
	case _, ok := <-an.proposeC:
		if ok {
			t.Fatal("proposeC should be closed")
		}
	default:
		t.Fatal("proposeC close was not observable")
	}
	select {
	case _, ok := <-an.confChangeC:
		if ok {
			t.Fatal("confChangeC should be closed")
		}
	default:
		t.Fatal("confChangeC close was not observable")
	}
	select {
	case _, ok := <-an.kvServiceStopC:
		if ok {
			t.Fatal("kvServiceStopC should be closed")
		}
	default:
		t.Fatal("kvServiceStopC close was not observable")
	}
}
