package transport

import (
	"github.com/Mulily0513/C2KV/src/log"
	"github.com/Mulily0513/C2KV/src/pb"
	"github.com/Mulily0513/C2KV/src/transport/types"
	"net"
	"strings"
)

type RaftOperator interface {
	Process(m *pb.Message)
}

//go:generate mockgen -source=./transport.go -destination=./mocks/transport.go -package=mock
type Transporter interface {
	ListenPeer(ln net.Listener)

	Send(m []*pb.Message)

	AddPeer(id types.ID, IAddr string)

	Stop()
}

type Transport struct {
	LocalId    types.ID
	LocalIAddr string

	Peers map[types.ID]Peer

	RaftOperator RaftOperator

	StopC chan struct{}
}

func (t *Transport) ListenPeer(ln net.Listener) {
	for {
	flag:
		conn, err := ln.Accept()
		if err != nil {
			log.Errorf("Accept tcp err:%v", err)
			continue
		}

		remoteAddr := strings.Split(conn.RemoteAddr().String(), ":")
		for _, v := range t.Peers {
			p := v.(*peer)
			if len(remoteAddr) == 2 && strings.Contains(p.peerAddr, remoteAddr[0]) {
				v.AttachConn(conn)
				goto flag
			}
		}
		log.Warnf("get wrong conn(remote ip addr:%s) drop it", remoteAddr)
		conn.Close()
	}
}

func (t *Transport) AddPeer(peerID types.ID, peerIAddr string) {
	receiveC := make(chan *pb.Message, recvBufSize)
	netErrC := make(chan error, 1)
	p := &peer{
		localId:    t.LocalId,
		localIAddr: t.LocalIAddr,
		peerId:     peerID,
		peerAddr:   peerIAddr,
		raft:       t.RaftOperator,
		recvC:      receiveC,
		stopC:      t.StopC,
	}
	t.Peers[peerID] = p
	p.streamReader = startStreamReader(t.LocalId, peerID, peerIAddr, t.LocalIAddr, netErrC, receiveC)
	p.streamWriter = startStreamWriter(t.LocalId, peerID, peerIAddr, t.LocalIAddr, netErrC)
	go func() {
		for {
			select {
			case mm := <-p.recvC:
				p.raft.Process(mm)
			case <-p.stopC:
				return
			}
		}
	}()
}

func (t *Transport) Send(msgs []*pb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		to := types.ID(m.To)
		p, ok := t.Peers[to]
		if ok {
			p.Send(m)
			continue
		}
	}
}

func (t *Transport) Stop() {

}
