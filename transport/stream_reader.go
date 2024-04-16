package transport

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
)

type streamReader struct {
	localId  types.ID
	peerId   types.ID
	peerIp   string
	enc      *msgDecoderAndReader
	receiveC chan *pb.Message //从peer中获取对端节点发送过来的消息，然后交给raft算法层进行处理，只接收非prop信息
	netErrC  chan error
	ErrorC   chan error
}

func startStreamReader(localID, peerId types.ID, netErrC chan error, receiveC chan *pb.Message, peerIp string) *streamReader {
	r := &streamReader{
		localId:  localID,
		peerId:   peerId,
		peerIp:   peerIp,
		receiveC: receiveC,
		netErrC:  netErrC,
	}
	go r.run()
	return r
}

func (cr *streamReader) run() {
	cr.enc = cr.dial()
	for {
		m, err := cr.enc.decodeAndRead()
		if err != nil {
			log.Errorf("failed read from conn %s,%v", cr.enc.r.RemoteAddr(), err)
			cr.close()
			cr.enc = cr.dial()
			continue
		}
		select {
		case <-cr.netErrC:
			cr.enc = cr.dial()
			continue
		case cr.receiveC <- m:
		}
	}
}

func (cr *streamReader) dial() *msgDecoderAndReader {
	var count int
	for {
		Conn, err := net.Dial("tcp", cr.peerIp)
		if err != nil {
			log.Errorf("start dial remote peer from %s to %s failed %v", cr.localId.Str(), cr.peerId.Str(), err)
			//todo 一直重连不成功
			count++
			continue
		}
		log.Infof("start dial remote peer from %s to %s success", cr.localId.Str(), cr.peerId.Str())
		return &msgDecoderAndReader{Conn}
	}
}

func (cr *streamReader) close() {
	if cr.enc != nil {
		if err := cr.enc.r.Close(); err != nil {
			log.Error("failed to close remote peer connection").Str("local-member-id", cr.localId.Str()).Str("remote-peer-id", cr.peerId.Str()).Err("", err).Record()
		}
	}
	cr.enc = nil
}
