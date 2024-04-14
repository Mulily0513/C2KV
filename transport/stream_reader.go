package transport

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"io"
	"net"
	"sync"
)

type streamReader struct {
	localId types.ID

	peerId       types.ID
	peerIp       string
	peerStatus   *peerStatus
	mu           sync.Mutex
	paused       bool
	RaftOperator RaftOperator

	enc      *msgDecoderAndReader
	receiveC chan<- *pb.Message //从peer中获取对端节点发送过来的消息，然后交给raft算法层进行处理，只接收非prop信息
	errorC   chan<- error
	stopC    chan struct{}
}

func startStreamReader(localID, peerId types.ID, peerStatus *peerStatus, tr RaftOperator,
	errC chan error, receiveC chan *pb.Message, peerIp string) *streamReader {
	r := &streamReader{
		localId:      localID,
		peerId:       peerId,
		peerIp:       peerIp,
		peerStatus:   peerStatus,
		receiveC:     receiveC,
		stopC:        make(chan struct{}),
		errorC:       errC,
		RaftOperator: tr,
	}
	go r.run()
	return r
}

func (cr *streamReader) run() {
	cr.enc = cr.dial()
	for {
		m, err := cr.enc.decodeAndRead()
		if err != nil {
			if err == io.EOF {
				continue
			} else {
				log.Errorf("failed read from conn", err)
				cr.dial()
				continue
			}
		}
		cr.receiveC <- &m

		select {
		case <-cr.stopC:
			return
		default:
			continue
		}
	}
}

func (cr *streamReader) dial() *msgDecoderAndReader {
	for {
		Conn, err := net.Dial("tcp", cr.peerIp)
		if err != nil {
			log.Errorf("start dial remote peer from %s to %s failed %v", cr.localId.Str(), cr.peerId.Str(), err)
			continue
		}
		log.Infof("start dial remote peer from %s to %s success", cr.localId.Str(), cr.peerId.Str())
		return &msgDecoderAndReader{Conn}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.close()
	cr.mu.Unlock()
	<-cr.stopC
}

func (cr *streamReader) close() {
	if cr.enc != nil {
		if err := cr.enc.r.Close(); err != nil {
			log.Warn("failed to close remote peer connection").Str("local-member-id", cr.localId.Str()).
				Str("remote-peer-id", cr.peerId.Str()).Err("", err).Record()
		}
	}
	cr.enc = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}
