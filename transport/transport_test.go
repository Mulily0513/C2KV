package transport

import (
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/transport/types"
	"github.com/magiconair/properties/assert"
	"net"
	"strings"
	"testing"
	"time"
)

func TestListenPeerConn(t *testing.T) {
	initLog()
	trans := &Transport{
		LocalId: types.ID(1),
		Peers:   make(map[types.ID]Peer),
		StopC:   make(chan struct{}),
	}

	mockPeer1 := &peer{
		peerAddr:     "127.0.0.1:8080",
		streamWriter: &streamWriter{connC: make(chan net.Conn)},
	}
	mockPeer2 := &peer{
		peerAddr:     "172.16.60.33:8080",
		streamWriter: &streamWriter{connC: make(chan net.Conn)},
	}
	mockPeer3 := &peer{
		peerAddr:     "172.16.60.34:8080",
		streamWriter: &streamWriter{connC: make(chan net.Conn)},
	}
	trans.Peers[types.ID(1)] = mockPeer1
	trans.Peers[types.ID(2)] = mockPeer2
	trans.Peers[types.ID(3)] = mockPeer3

	go trans.ListenPeer("127.0.0.1:8080")

	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	// 等待一段时间以便ListenPeerConn可以处理连接
	time.Sleep(time.Second)

	tests := []struct {
		name string
		peer *peer
	}{
		{
			name: "success",
			peer: mockPeer1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go trans.ListenPeer("127.0.0.1:8080")
			time.Sleep(time.Second)
			conn, err := net.Dial("tcp", "127.0.0.1:8080")
			if err != nil {
				t.Fatalf("failed to dial: %v", err)
			}
			defer conn.Close()
			// 等待一段时间以便ListenPeerConn可以处理连接
			time.Sleep(time.Second)
			// 检查模拟的peer是否已经接收到了连接
			c := <-tt.peer.streamWriter.connC
			ipaddr := c.(*net.TCPConn).RemoteAddr()
			testip := strings.Split(ipaddr.String(), ":")[0]
			assert.Equal(t, testip, strings.Split(mockPeer1.peerAddr, ":")[0])
		})
	}
}

func initLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[Cold2DB]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}
