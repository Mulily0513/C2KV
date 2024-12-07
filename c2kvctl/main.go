package main

import (
	"context"
	"flag"
	"github.com/Mulily0513/C2KV/client"
)

func main() {
	var addr string
	var key string
	var value string
	flag.StringVar(&addr, "i", "", "c2kv grpc addr")
	flag.StringVar(&key, "k", "", "key")
	flag.StringVar(&value, "v", "", "value")
	flag.Parse()

	cli, err := client.NewClient(addr)
	if err != nil {
		return
	}

	putrsp, err := cli.Put(context.Background(), key, value)
	if err != nil {
		println(err.Error())
		return
	}

	if putrsp.Msg != "ok" {
		println(putrsp.Msg)
	} else {
		println(putrsp.Msg)
	}

	getrsp, err := cli.Get(context.Background(), key)
	if err != nil {
		println(err.Error())
		return
	}
	for _, item := range getrsp.Kvs {
		println(string(item.Key), string(item.Value))
	}
}
