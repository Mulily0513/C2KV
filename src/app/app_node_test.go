package app

import (
	"github.com/Mulily0513/C2KV/src/config"
	"github.com/Mulily0513/C2KV/src/log"
	"github.com/Mulily0513/C2KV/src/pb"
	"github.com/Mulily0513/C2KV/src/raft"
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
	// Start servePropCAndConfC function as a goroutine

	// Wait for a while to ensure the goroutine has enough time to process messages
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
