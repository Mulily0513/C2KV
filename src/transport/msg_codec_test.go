package transport

import (
	"fmt"
	"github.com/Mulily0513/C2KV/src/config"
	"github.com/Mulily0513/C2KV/src/log"
	"github.com/Mulily0513/C2KV/src/pb"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

var (
	message1 = &pb.Message{
		Type:       pb.MsgProp,
		To:         1,
		From:       2,
		Term:       4,
		LogTerm:    4,
		Index:      101,
		Commit:     100,
		Reject:     true,
		RejectHint: 100,
		Entries: []pb.Entry{
			{
				Term:  4,
				Index: 101,
				Type:  pb.EntryNormal,
				Data:  []byte{1, 2},
			},
		},
	}

	message2 = &pb.Message{
		Type:       pb.MsgProp,
		To:         11,
		From:       21,
		Term:       41,
		LogTerm:    41,
		Index:      102,
		Commit:     100,
		Reject:     true,
		RejectHint: 100,
		Entries: []pb.Entry{
			{
				Term:  4,
				Index: 101,
				Type:  pb.EntryNormal,
				Data:  []byte{1, 2},
			},
		},
	}
)

func InitLog() {
	cfg := &config.ZapConfig{
		Level:        "debug",
		Format:       "console",
		Prefix:       "[C2KV]",
		Director:     "./log",
		ShowLine:     true,
		EncodeLevel:  "LowercaseColorLevelEncoder",
		LogInConsole: true,
	}
	log.InitLog(cfg)
}

func TestDataPack(t *testing.T) {
	InitLog()
	signal := make(chan struct{})
	go func() {
		listener, err := net.Listen("tcp", "127.0.0.1:7777")
		if err != nil {
			fmt.Println("server listen err:", err)
			return
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("server accept err:", err)
			}

			//处理客户端请求
			go func(conn net.Conn) {
				for {
					dec := &msgDecoderAndReader{conn}
					pbmsg, err := dec.decodeAndRead()
					if err != nil {
						t.Log(err)
						continue
					}

					if pbmsg.Index == message1.Index {
						assert.Equal(t, pbmsg, message1)
					}

					if pbmsg.Index == message2.Index {
						assert.Equal(t, pbmsg, message2)
					}

					signal <- struct{}{}
				}
			}(conn)
		}
	}()

	go func() {
		conn, err := net.Dial("tcp", "127.0.0.1:7777")
		if err != nil {
			fmt.Println("client dial err:", err)
			return
		}

		enc := &msgEncoderAndWriter{conn}

		enc.encodeAndWrite(message1)
		enc.encodeAndWrite(message2)
	}()

	select {
	case <-signal:
		return
	}
}
