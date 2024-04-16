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

	Send(m []*pb.Message)

	AddPeer(id types.ID, url string)

	Stop()
}

type Transport struct {
	LocalId    types.ID
	LocalIAddr string

	Peers map[types.ID]Peer

	RaftOperator RaftOperator

	StopC chan struct{}
}

func (t *Transport) ListenPeer(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panic("start listening failed").Str("local id", t.LocalId.Str()).Str("addr", addr).Err("err", err).Record()
	}
	log.Info("start listening").Str("local id", t.LocalId.Str()).Str("addr", addr).Record()

	for {
	flag:
		conn, err := ln.Accept()
		if err != nil {
			log.Error("Accept tcp err").Record()
		}

		remoteAddr := strings.Split(conn.RemoteAddr().String(), ":")

		for _, v := range t.Peers {
			p := v.(*peer)
			if len(remoteAddr) == 2 && strings.Contains(p.peerAddr, remoteAddr[0]) {
				v.AttachConn(conn)
				goto flag
			}
		}
		log.Warnf("get wrong conn the remote ip addr:%s drop it", remoteAddr)
		conn.Close()
	}
}

func (t *Transport) AddPeer(peerID types.ID, peerAddr string) {
	receiveC := make(chan *pb.Message, recvBufSize)
	netErrC := make(chan error, 1)
	streamReader := startStreamReader(t.LocalId, peerID, netErrC, receiveC, peerAddr)
	streamWriter := startStreamWriter(t.LocalId, peerID, netErrC, peerAddr)
	p := &peer{
		localId:      t.LocalId,
		localIAddr:   t.LocalIAddr,
		peerId:       peerID,
		peerAddr:     peerAddr,
		raft:         t.RaftOperator,
		streamWriter: streamWriter,
		streamReader: streamReader,
		recvC:        receiveC,
	}
	go func() {
		for {
			select {
			case mm := <-p.recvC:
				if err := p.raft.Process(mm); err != nil {
					log.Warn("failed to process Raft message").Err(code.MessageProcErr, err)
				}
			case <-p.stopC:
				return
			}
		}
	}()
	t.Peers[peerID] = p
	log.Info("add remote peer success").Str(code.LocalId, t.LocalId.Str()).Str(code.RemoteId, peerID.Str()).Record()
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
