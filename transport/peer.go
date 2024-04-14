package transport

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
	"sync"
	"time"
)

const (
	// ConnReadTimeout 这段代码定义了两个常量，DefaultConnReadTimeout 和 DefaultConnWriteTimeout，它们分别表示每个连接的读取和写入超时时间。在 rafthttp 包中创建连接时，会设置这两个超时时间。
	ConnReadTimeout  = 5 * time.Second
	ConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096

	//这段注释是关于在一次 leader 选举过程中，最多可以容纳多少个 proposal 的说明。一般来说，一次 leader 选举最多需要 1 秒钟，可能会有 0-2 次选举冲突，每次冲突需要 0.5 秒钟。
	//我们假设并发 proposer 的数量小于 4096，因为一个 client 的 proposal 至少需要阻塞 1 秒钟，所以 4096 足以容纳所有的 proposals。
	maxPendingProposals = 4096
)

//go:generate mockgen -source=./peer.go -destination=./mocks/peer.go -package=mock
type Peer interface {
	Send(m pb.Message)

	AttachConn(conn net.Conn)
}

type peer struct {
	localId  types.ID
	remoteId types.ID
	peerIp   string

	raft         RaftOperator
	status       *peerStatus
	streamWriter *streamWriter
	streamReader *streamReader

	recvC chan *pb.Message //从Stream消息通道中读取到消息之后，会通过该通道将消息交给Raft接口，然后由它返回给底层etcd-raft模块进行处理

	mu     sync.Mutex
	paused bool
	stopc  chan struct{}
}

func (p *peer) handleReceiveC() {
	for {
		select {
		case mm := <-p.recvC:
			if err := p.raft.Process(mm); err != nil {
				log.Warn("failed to process Raft message").Err(code.MessageProcErr, err)
			}
		case <-p.stopc:
			return
		}
	}
}

func (p *peer) Send(m pb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()
	if paused {
		return
	}

	writeC := p.streamWriter.writeC()
	select {
	case writeC <- m:
	default:
		if p.status.isActive() {
			log.Warn(
				"dropped internal Raft message since sending buffer is full (overloaded network)").
				Str("message-type", m.Type.String()).
				Str("local-member-id", p.localId.Str()).
				Str("from", types.ID(m.From).Str()).
				Str("remote-peer-id", p.remoteId.Str()).
				Bool("remote-peer-active", p.status.isActive()).Record()
		}
	}
}

func (p *peer) AttachConn(conn net.Conn) {
	p.streamWriter.connC <- conn
}

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	peerId types.ID
	mu     sync.Mutex // protect variables below
	active bool
	since  time.Time
}

func newPeerStatus(id types.ID) *peerStatus {
	return &peerStatus{peerId: id}
}

func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		log.Info("peer became active").Str("peer-id", s.peerId.Str())
		s.active = true
		s.since = time.Now()
	}
}

func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("failed to %s %d on %s (%s)", failure.action, s.peerId, failure.source, reason)
	if s.active {
		log.Warn("peer became inactive (message send to peer failed)").Str("peer-id", s.peerId.Str()).Err("", errors.New(msg)).Record()
		s.active = false
		s.since = time.Time{}
		return
	}
}

func (s *peerStatus) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *peerStatus) activeSince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.since
}
