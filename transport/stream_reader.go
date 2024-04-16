package transport

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
)

type streamReader struct {
	localId    types.ID
	peerId     types.ID
	localIAddr string
	peerIAddr  string
	enc        *msgDecoderAndReader
	receiveC   chan *pb.Message
	netErrC    chan error
}

func startStreamReader(localID, peerId types.ID, peerIAddr, localIAddr string, netErrC chan error, receiveC chan *pb.Message) *streamReader {
	r := &streamReader{
		localId:    localID,
		localIAddr: localIAddr,
		peerId:     peerId,
		peerIAddr:  peerIAddr,
		receiveC:   receiveC,
		netErrC:    netErrC,
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
		Conn, err := net.Dial("tcp", cr.peerIAddr)
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
