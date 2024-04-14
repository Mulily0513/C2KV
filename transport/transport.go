package transport

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
	"strings"
)

type RaftOperator interface {
	Process(m *pb.Message) error
	ReportUnreachable(id uint64)
}

//go:generate mockgen -source=./transport.go -destination=../mocks/transport.go -package=mock
type Transporter interface {
	ListenPeer(localIp string)

	Send(m []pb.Message)

	AddPeer(id types.ID, url string)
}

type Transport struct {
	LocalID types.ID

	Peers map[types.ID]Peer

	RaftOperator RaftOperator

	ErrorC chan error
	StopC  chan struct{}
}

// AddPeer peer 相当于是其他节点在本地的代言人，本地节点发送消息给其他节点实质是将消息递给peer由peer发送给对端节点
func (t *Transport) AddPeer(peerID types.ID, peerIp string) {
	receiveC := make(chan *pb.Message, recvBufSize)
	peerStatus := newPeerStatus(peerID)
	streamReader := startStreamReader(t.LocalID, peerID, peerStatus, t.RaftOperator, t.ErrorC, receiveC, peerIp)
	streamWriter := startStreamWriter(t.LocalID, peerID, peerStatus, t.RaftOperator, t.ErrorC, peerIp)
	p := &peer{
		localId:      t.LocalID,
		remoteId:     peerID,
		peerIp:       peerIp,
		raft:         t.RaftOperator,
		status:       peerStatus,
		streamWriter: streamWriter,
		streamReader: streamReader,
		recvC:        receiveC,
		stopc:        t.StopC,
	}
	go p.handleReceiveC()
	t.Peers[peerID] = p
	log.Info("add remote peer success").Str(code.LocalId, t.LocalID.Str()).Str(code.RemoteId, peerID.Str()).Record()
}

func (t *Transport) ListenPeer(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panic("start listening failed").Str("local id", t.LocalID.Str()).Str("addr", addr).Err("err", err).Record()
		return
	}
	log.Info("start listening").Str("local id", t.LocalID.Str()).Str("addr", addr).Record()

	for {
	flag:
		conn, err := ln.Accept()
		if err != nil {
			log.Error("Accept tcp err").Record()
		}

		remoteAddr := strings.Split(conn.RemoteAddr().String(), ":")

		for _, v := range t.Peers {
			p := v.(*peer)
			if len(remoteAddr) == 2 && strings.Contains(p.peerIp, remoteAddr[0]) {
				v.AttachConn(conn)
				goto flag
			}
		}
		log.Errorf("get wrong conn the remote ip addr:%s drop it", remoteAddr)
		conn.Close()
	}
}

func (t *Transport) Send(msgs []pb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		to := types.ID(m.To)
		p, pok := t.Peers[to]
		if pok {
			p.Send(m)
			continue
		}
	}
}
