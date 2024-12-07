package transport

import (
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/transport/types"
	"net"
)

const (
	recvBufSize = 4096
)

//go:generate mockgen -source=./peer.go -destination=./mocks/peer.go -package=mock
type Peer interface {
	Send(m *pb.Message)

	AttachConn(conn net.Conn)

	Stop()
}

type peer struct {
	localId    types.ID
	localIAddr string
	peerId     types.ID
	peerAddr   string

	raft         RaftOperator
	streamWriter *streamWriter
	streamReader *streamReader

	//After reading a message from the Stream message channel, the message will be passed to the Raft interface through this channel.
	//and then it will be returned to the underlying etcd-raft module for processing.
	recvC chan *pb.Message
	stopC chan struct{}
}

func (p *peer) Send(m *pb.Message) {
	select {
	case p.streamWriter.msgC <- m:
	default:
		log.Errorf("dropped internal Raft message since sending buffer is full (overloaded network),message:%+v, local ip : %s, remote ip : %s", *m, p.localIAddr, p.peerAddr)
	}
}

func (p *peer) AttachConn(conn net.Conn) {
	p.streamWriter.connC <- conn
}

func (p *peer) Stop() {

}
