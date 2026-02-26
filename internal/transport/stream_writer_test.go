package transport

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/Mulily0513/C2KV/api/gen/pb"
	"github.com/Mulily0513/C2KV/internal/transport/types"
)

func TestStreamWriterBatchOverflowDoesNotDropNextMessage(t *testing.T) {
	netErrC := make(chan error, 1)
	w := startStreamWriter(types.ID(1), types.ID(2), "127.0.0.1:2", "127.0.0.1:1", netErrC)

	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	w.connC <- left
	dec := &msgDecoderAndReader{r: right}

	bigPayload := bytes.Repeat([]byte("x"), 80*1024)
	msg1 := &pb.Message{
		Type: pb.MsgApp,
		From: 1,
		To:   2,
		Entries: []pb.Entry{
			{Index: 1, Term: 1, Data: bigPayload},
		},
	}
	msg2 := &pb.Message{
		Type: pb.MsgApp,
		From: 1,
		To:   2,
		Entries: []pb.Entry{
			{Index: 2, Term: 1, Data: bigPayload},
		},
	}

	w.msgC <- msg1
	w.msgC <- msg2

	_ = right.SetReadDeadline(time.Now().Add(2 * time.Second))
	got1, err := dec.decodeAndRead()
	if err != nil {
		t.Fatalf("decode first msg failed: %v", err)
	}
	got2, err := dec.decodeAndRead()
	if err != nil {
		t.Fatalf("decode second msg failed: %v", err)
	}
	if len(got1.Entries) == 0 || len(got2.Entries) == 0 {
		t.Fatalf("decoded empty entries, got1=%+v got2=%+v", got1, got2)
	}
	if got1.Entries[0].Index != 1 || got2.Entries[0].Index != 2 {
		t.Fatalf("messages out of order or dropped, got1=%d got2=%d", got1.Entries[0].Index, got2.Entries[0].Index)
	}
}
