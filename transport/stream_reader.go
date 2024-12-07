package transport

import (
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/transport/types"
	"net"
	"time"
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
	for {
		Conn, err := net.Dial("tcp", cr.peerIAddr)
		if err != nil {
			//todo
			time.Sleep(time.Second * 5)
			log.Warnf("dial remote peer from %s to %s failed, error:%v", cr.localIAddr, cr.peerIAddr, err)
			continue
		}
		log.Infof("dial tcp remote peer from %s to %s success, start streamReader", cr.localIAddr, cr.peerIAddr)
		return &msgDecoderAndReader{Conn}
	}
}

func (cr *streamReader) close() {
	if cr.enc != nil {
		if err := cr.enc.r.Close(); err != nil {
			log.Errorf("failed to close remote peer connection, local ip : %s, remote ip : %s ", cr.localIAddr, cr.peerIAddr)
		}
	}
}
