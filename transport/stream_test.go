package transport

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"io"
	"sync"
	"testing"
)

func TestStreamWriterAndReader_Run(t *testing.T) {
	initLog()
	localID := types.ID(1)
	peerID := types.ID(2)
	reciveC := make(chan *pb.Message)
	errC := make(chan error)
	peerIP := "127.0.0.1:7878"
	stopC := make(chan struct{})

	writer := &streamWriter{
		msgC:   make(chan *pb.Message),
		connC:  make(chan io.WriteCloser),
		stopC:  make(chan struct{}),
		done:   make(chan struct{}),
		mu:     sync.Mutex{},
		status: status,
	}

	go func() {
		ln, err := NewStoppableListener(peerIP, stopC)
		if err != nil {
			log.Errorf("listen failed", err)
		}
		for {
			conn, _ := ln.Accept()
			log.Info("get a conn").Str("conn", conn.RemoteAddr().String()).Record()
		}
	}()

	reader := startStreamReader(localID, peerID, , nil, errC, reciveC, peerIP)
	go reader.run()

	writer.run()
}
