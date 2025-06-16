package app

import (
	"context"
	"fmt"
	"github.com/Mulily0513/C2KV/client"
	"github.com/Mulily0513/C2KV/src/db/mocks"
	"testing"
	"time"
)

func TestKVService_Put(t *testing.T) {
	proposeC := make(chan []byte, 1000)
	monitorKV := make(map[string]chan struct{})
	requestTimeOut := 1000
	s := &KvService{
		Storage:    &mocks.MockStorage{},
		proposeC:   proposeC,
		monitorKV:  monitorKV,
		ReqTimeout: time.Duration(requestTimeOut) * time.Millisecond,
	}
	go ServeGRPCKVAPI(s, "127.0.0.1:8888")

	cli, err := client.NewClient("127.0.0.1:8888")
	if err != nil {
		t.Error(err)
		return
	}

	rsp, err := cli.Put(context.Background(), "key", "value")
	if err != nil {
		t.Error(err)
	}
	println(fmt.Sprintf("%+v", rsp))
}
