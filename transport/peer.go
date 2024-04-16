package transport

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
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

	recvC chan *pb.Message //从Stream消息通道中读取到消息之后，会通过该通道将消息交给Raft接口，然后由它返回给底层etcd-raft模块进行处理
	stopC chan struct{}
}

func (p *peer) Send(m *pb.Message) {
	select {
	case p.streamWriter.msgC <- m:
	default:
		log.Error(
			"dropped internal Raft message since sending buffer is full (overloaded network)").
			Str("message-type", m.Type.String()).
			Str("local-member-id", p.localId.Str()).
			Str("from", types.ID(m.From).Str()).
			Str("peer-peer-id", p.peerId.Str()).Record()
	}
}

func (p *peer) AttachConn(conn net.Conn) {
	p.streamWriter.connC <- conn
}

func (p *peer) Stop() {

}
