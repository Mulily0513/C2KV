package transport

import (
	"github.com/Mulily0513/C2KV/src/pb"
)

var (
	message = &pb.Message{
		Type:       pb.MsgProp,
		To:         2,
		From:       1,
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
)
