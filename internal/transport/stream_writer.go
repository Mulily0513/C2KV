package transport

import (
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/Mulily0513/C2KV/internal/transport/types"
	"net"
)

const (
	streamBufSize       = 8192
	streamBatchMaxMsgs  = 32
	streamBatchMaxBytes = 256 * 1024
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
	var pending *pb.Message
	for {
		select {
		case conn := <-cw.connC:
			cw.close()
			cw.enc = &msgEncoderAndWriter{conn}
			msgC = cw.msgC
			pending = nil
			log.Infof("tcp conn access success, start streamWriter, local ip : %s, remote ip : %s", cw.localIAddr, cw.peerIAddr)
			continue
		default:
		}

		if msgC == nil {
			conn := <-cw.connC
			cw.close()
			cw.enc = &msgEncoderAndWriter{conn}
			msgC = cw.msgC
			pending = nil
			log.Infof("tcp conn access success, start streamWriter, local ip : %s, remote ip : %s", cw.localIAddr, cw.peerIAddr)
			continue
		}

		var (
			m  *pb.Message
			ok bool
		)
		if pending != nil {
			m = pending
			pending = nil
			ok = true
		} else {
			m, ok = <-msgC
		}
		if !ok {
			// Channel closed; pause writer loop until reconnection or shutdown.
			msgC = nil
			pending = nil
			continue
		}
		if m == nil {
			continue
		}

		batchBytes := HeaderLength + m.Size()
		batch := make([]*pb.Message, 0, streamBatchMaxMsgs)
		batch = append(batch, m)
		draining := true
		for draining && len(batch) < streamBatchMaxMsgs {
			select {
			case next, open := <-msgC:
				if !open {
					msgC = nil
					draining = false
					continue
				}
				if next == nil {
					continue
				}
				nextBytes := HeaderLength + next.Size()
				if batchBytes+nextBytes > streamBatchMaxBytes {
					// Keep overflow message for next write turn instead of dropping it.
					pending = next
					draining = false
					continue
				}
				batch = append(batch, next)
				batchBytes += nextBytes
			default:
				draining = false
			}
		}

		if _, err := cw.enc.encodeAndWriteBatch(batch); err != nil {
			select {
			case cw.netErrC <- err:
			default:
			}
			log.Errorf("lost tcp streaming connection , local ip : %s remote ip : %s ,err : %v", cw.localIAddr, cw.peerIAddr, err)
			cw.close()
			// Pause consuming messages until a new connection arrives.
			msgC = nil
			pending = nil
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
