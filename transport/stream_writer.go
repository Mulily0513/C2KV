package transport

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
)

const (
	streamBufSize = 4096
)

type streamWriter struct {
	localID types.ID
	peerID  types.ID
	peerIp  string

	enc     *msgEncoderAndWriter
	msgC    chan *pb.Message //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC   chan net.Conn    //通过该通道获取当前streamWriter实例关联的底层网络连接
	netErrC chan error
}

func startStreamWriter(localId, peerId types.ID, netErrC chan error, peerIp string) *streamWriter {
	w := &streamWriter{
		localID: localId,
		peerID:  peerId,
		peerIp:  peerIp,
		msgC:    make(chan *pb.Message, streamBufSize),
		connC:   make(chan net.Conn),
		netErrC: netErrC,
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var msgC chan *pb.Message
	log.Info("started stream writer run").Str(code.LocalId, cw.localID.Str()).Str(code.RemoteId, cw.peerID.Str()).Str(code.RemoteIp, cw.peerIp).Record()
	for {
		select {
		case m := <-msgC:
			_, err := cw.enc.encodeAndWrite(m)
			if err != nil {
				cw.netErrC <- err
				log.Error("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).Str(code.RemoteId, cw.peerID.Str()).Record()
				cw.close()
			}
		case conn := <-cw.connC:
			//todo 接收到新链接时需要断开旧的链接?
			cw.enc = &msgEncoderAndWriter{conn}
			msgC = cw.msgC
			log.Info("established TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).Str(code.RemoteId, cw.peerID.Str()).Record()
		}
	}
}

func (cw *streamWriter) close() {
	if cw.enc != nil {
		if err := cw.enc.w.Close(); err != nil {
			log.Error("failed to close remote peer connection").Str("local-member-id", cw.localI.Str()).Str("remote-peer-id", cr.peerId.Str()).Err("", err).Record()
		}
	}
	cw.enc = nil
}
