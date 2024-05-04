package app

import (
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/raft"
	"github.com/golang/mock/gomock"
	"testing"
	"time"
)

var (
	ready1 = raft.Ready{
		HardState: pb.HardState{},
		Messages:  []*pb.Message{message1, message2},
	}

	entries = []pb.Entry{
		{
			Term:  4,
			Index: 101,
			Type:  pb.EntryNormal,
			Data:  []byte{1, 2, 3},
		},
		{
			Term:  4,
			Index: 101,
			Type:  pb.EntryNormal,
			Data:  []byte{1, 2, 3},
		},
	}

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
		Entries:    entries,
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
		Entries:    entries,
	}
)

func initLog() {
	cfg := &config.ZapConfig{
		Level:        "debug",
		Format:       "console",
		Prefix:       "[Cold2DB]",
		Director:     "./log",
		ShowLine:     true,
		EncodeLevel:  "LowercaseColorLevelEncoder",
		LogInConsole: true,
	}
	log.InitLog(cfg)
}

func TestAppNode_ServePropCAndConfC(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	// 启动servePropCAndConfC函数作为goroutine

	// 等待一段时间以确保goroutine有足够的时间处理消息
	time.Sleep(time.Millisecond * 100)
}

func TestAppNode_ServeRaftNode(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
}

func TestAppNode_ApplyEntries(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	t.Log("apply success")
}
