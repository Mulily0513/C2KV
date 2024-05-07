package transport

import (
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/transport/types"
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

	enc     *msgEncoderAndWriter
	msgC    chan *pb.Message //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC   chan net.Conn    //通过该通道获取当前streamWriter实例关联的底层网络连接
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
				log.Errorf("lost TCP streaming connection , local ip : %s remote ip : %s ,err : %v", cw.localIAddr, cw.peerIAddr, err)
				cw.close()
			}
		case conn := <-cw.connC: //"attachconn执行后，connC中有数据，然后发数据"
			cw.close()
			cw.enc = &msgEncoderAndWriter{conn}
			msgC = cw.msgC
			log.Infof("TCP connection access success, start streamWriter, local ip : %s, remote ip : %s", cw.localIAddr, cw.peerIAddr)
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
