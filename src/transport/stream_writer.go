package transport

import (
	"github.com/Mulily0513/C2KV/src/log"
	"github.com/Mulily0513/C2KV/src/pb"
	"github.com/Mulily0513/C2KV/src/transport/types"
	"net"
)

const (
	streamBufSize = 4096
)

type streamWriter struct {
	localId    types.ID
	peerId     types.ID
	localIAddr string
	peerIAddr  string

	enc *msgEncoderAndWriter
	// Peer will write the message to be sent into this channel, and streamWriter will read the message from this channel and send it out.
	msgC chan *pb.Message
	// Obtain the underlying network connection associated with the current streamWriter instance through this channel.
	connC   chan net.Conn
	netErrC chan error
}

func startStreamWriter(localId, peerId types.ID, peerIAddr, localIAddr string, netErrC chan error) *streamWriter {
	w := &streamWriter{
		localId:    localId,
		peerId:     peerId,
		localIAddr: localIAddr,
		peerIAddr:  peerIAddr,
		msgC:       make(chan *pb.Message, streamBufSize),
		connC:      make(chan net.Conn),
		netErrC:    netErrC,
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var msgC chan *pb.Message
	for {
		select {
		case m := <-msgC:
			if _, err := cw.enc.encodeAndWrite(m); err != nil {
				cw.netErrC <- err
				log.Errorf("lost tcp streaming connection , local ip : %s remote ip : %s ,err : %v", cw.localIAddr, cw.peerIAddr, err)
				cw.close()
			}
		case conn := <-cw.connC:
			cw.close()
			cw.enc = &msgEncoderAndWriter{conn}
			msgC = cw.msgC
			log.Infof("tcp conn access success, start streamWriter, local ip : %s, remote ip : %s", cw.localIAddr, cw.peerIAddr)
		}
	}
}

func (cw *streamWriter) close() {
	if cw.enc != nil {
		if err := cw.enc.w.Close(); err != nil {
			log.Errorf("failed to close remote peer connection, local ip : %s, remote ip : %s, err : %v", cw.localIAddr, cw.peerIAddr, err)
		}
	}
	cw.enc = nil
}
