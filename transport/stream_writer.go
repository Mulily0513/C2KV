package transport

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
	"sync"
)

const (
	streamBufSize = 4096
)

type streamWriter struct {
	localID types.ID
	peerID  types.ID
	peerIp  string

	enc    *msgEncoderAndWriter
	status *peerStatus
	r      RaftOperator
	mu     sync.Mutex // guard field working and enc
	paused bool

	msgC   chan pb.Message //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC  chan net.Conn   //通过该通道获取当前streamWriter实例关联的底层网络连接
	stopC  chan struct{}
	done   chan struct{}
	errorC chan error
}

func startStreamWriter(local, id types.ID, status *peerStatus, r RaftOperator, errorC chan error, peerIp string) *streamWriter {
	w := &streamWriter{
		localID: local,
		peerID:  id,
		status:  status,
		peerIp:  peerIp,
		r:       r,
		msgC:    make(chan pb.Message, streamBufSize),
		connC:   make(chan net.Conn),
		stopC:   make(chan struct{}),
		done:    make(chan struct{}),
		errorC:  errorC,
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var msgC chan pb.Message
	log.Info("started stream writer run").Str(code.LocalId, cw.localID.Str()).
		Str(code.RemoteId, cw.peerID.Str()).Str(code.RemoteIp, cw.peerIp).Record()
	for {
		select {
		case m := <-msgC:
			err := cw.enc.encodeAndWrite(m)
			if err != nil {
				cw.status.deactivate(failureType{source: cw.localID.Str(), action: "write"}, err.Error())
				cw.close()
				msgC = nil
				cw.r.ReportUnreachable(m.To)
				log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
			}
		case conn := <-cw.connC:
			//todo 接收到新链接时需要断开旧的链接
			cw.enc = &msgEncoderAndWriter{conn}
			cw.status.activate()
			msgC = cw.msgC
			log.Info("established TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).Str(code.RemoteId, cw.peerID.Str()).Record()
		case <-cw.stopC:
			if cw.close() {
				log.Info("close TCP streaming connection with remote peer").Str(code.RemoteId, cw.peerID.Str()).Record()
			}
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writeC() chan<- pb.Message {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgC
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if cw.enc == nil {
		return false
	}
	if err := cw.enc.w.Close(); err != nil {
		log.Errorf("failed to close exited conn", err)
		return false
	}
	return true
}

func (cw *streamWriter) stop() {
	close(cw.stopC)
	<-cw.done
}

func (cw *streamWriter) pause() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.paused = true
}

func (cw *streamWriter) resume() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.paused = false
}
