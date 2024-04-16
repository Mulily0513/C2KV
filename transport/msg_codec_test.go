package transport

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

var (
	message1 = pb.Message{
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

	message2 = pb.Message{
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

// 测试粘包下的编解码
func TestDataPack(t *testing.T) {
	//创建socket TCP Server
	listener, err := net.Listen("tcp", "127.0.0.1:7777")
	if err != nil {
		fmt.Println("server listen err:", err)
		return
	}

	//创建服务器gotoutine，负责从客户端goroutine读取粘包的数据，然后进行解析
	go func() {
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
				}
			}(conn)
		}
	}()

	//客户端goroutine，负责模拟粘包的数据，然后进行发送
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

	//客户端阻塞
	select {
	case <-time.After(time.Second):
		return
	}
}
