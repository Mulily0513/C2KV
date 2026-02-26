package transport

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/Mulily0513/C2KV/internal/transport/types"
	"net"
)

const (
	recvBufSize = 8192
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
	if p.streamWriter == nil {
		return
	}
	select {
	case p.streamWriter.msgC <- m:
	case <-p.stopC:
		log.Warnf("stop sending raft message due to transport shutdown, message:%+v, local ip : %s, remote ip : %s", *m, p.localIAddr, p.peerAddr)
	}
}

func (p *peer) AttachConn(conn net.Conn) {
	p.streamWriter.connC <- conn
}

func (p *peer) Stop() {

}
