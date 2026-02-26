package transport

import (
	"bytes"
	"fmt"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"testing"
	"time"
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

func TestDecodeAndReadFragmentedPacket(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	raw, err := message1.Marshal()
	assert.NoError(t, err)
	packet := newPackage(DefaultId, raw).marshal()

	go func() {
		_, _ = client.Write(packet[:2])
		time.Sleep(5 * time.Millisecond)
		_, _ = client.Write(packet[2:5])
		time.Sleep(5 * time.Millisecond)
		_, _ = client.Write(packet[5:])
	}()

	dec := &msgDecoderAndReader{r: server}
	got, err := dec.decodeAndRead()
	assert.NoError(t, err)
	assert.Equal(t, message1, got)
}

func TestDecodeAndReadInvalidPayload(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	packet := newPackage(DefaultId, []byte{0x00}).marshal()
	go func() {
		_, _ = client.Write(packet)
	}()

	dec := &msgDecoderAndReader{r: server}
	_, err := dec.decodeAndRead()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal message failed")
}

func TestEncodeAndWriteShortWriter(t *testing.T) {
	raw, err := message2.Marshal()
	assert.NoError(t, err)
	expected := newPackage(DefaultId, raw).marshal()

	conn := &shortWriteConn{maxChunk: 3}
	enc := &msgEncoderAndWriter{w: conn}
	n, err := enc.encodeAndWrite(message2)
	assert.NoError(t, err)
	assert.Equal(t, len(expected), n)
	assert.Equal(t, expected, conn.buf.Bytes())
}

func TestEncodeAndWriteBatchShortWriter(t *testing.T) {
	raw1, err := message1.Marshal()
	assert.NoError(t, err)
	raw2, err := message2.Marshal()
	assert.NoError(t, err)
	expected := append(newPackage(DefaultId, raw1).marshal(), newPackage(DefaultId, raw2).marshal()...)

	conn := &shortWriteConn{maxChunk: 5}
	enc := &msgEncoderAndWriter{w: conn}
	n, err := enc.encodeAndWriteBatch([]*pb.Message{message1, message2})
	assert.NoError(t, err)
	assert.Equal(t, len(expected), n)
	assert.Equal(t, expected, conn.buf.Bytes())
}

type shortWriteConn struct {
	buf      bytes.Buffer
	maxChunk int
}

func (c *shortWriteConn) Read(_ []byte) (int, error) { return 0, io.EOF }

func (c *shortWriteConn) Write(p []byte) (int, error) {
	n := c.maxChunk
	if n <= 0 || n > len(p) {
		n = len(p)
	}
	return c.buf.Write(p[:n])
}

func (c *shortWriteConn) Close() error { return nil }

func (c *shortWriteConn) LocalAddr() net.Addr { return dummyAddr("local") }

func (c *shortWriteConn) RemoteAddr() net.Addr { return dummyAddr("remote") }

func (c *shortWriteConn) SetDeadline(_ time.Time) error { return nil }

func (c *shortWriteConn) SetReadDeadline(_ time.Time) error { return nil }

func (c *shortWriteConn) SetWriteDeadline(_ time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return "tcp" }

func (a dummyAddr) String() string { return string(a) }
