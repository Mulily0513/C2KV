package main

import (
	"flag"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "cfg", "./config.yaml", "c2kv config path")
	flag.Parse()
	config.InitConfig(cfgPath)
	log.InitLog(config.GetZapConf())
	raftConfig := config.GetRaftConf()
	kvStorage := db.OpenKVStorage(config.GetDBConf())

	proposeC := make(chan []byte, raftConfig.RequestLimit)
	confChangeC := make(chan pb.ConfChange)
	kvServiceStopC := make(chan struct{})
	monitorKV := make(map[int64]chan struct{})

	localIAddr, localEAddr, localId, peers := config.GetLocalInfo()
	StartAppNode(localId, localIAddr, peers, proposeC, confChangeC, kvServiceStopC, kvStorage, raftConfig, monitorKV)
	NewKVService(proposeC, raftConfig, kvStorage, monitorKV, localEAddr, kvServiceStopC)
}
