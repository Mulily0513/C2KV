package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

func main() {
	config.InitConfig()
	localIAddr, localId, nodes := config.GetLocalInfo()
	raftConfig := config.GetRaftConf()
	log.InitLog(config.GetZapConf())
	kvStorage := db.OpenKVStorage(config.GetDBConf())

	proposeC := make(chan []byte, raftConfig.RequestLimit)
	confChangeC := make(chan pb.ConfChange)
	kvServiceStopC := make(chan struct{})
	monitorKV := make(map[int64]chan struct{})

	kvStore := NewKVService(proposeC, confChangeC, raftConfig.RequestTimeout, kvStorage, monitorKV)
	StartAppNode(localId, nodes, proposeC, confChangeC, kvServiceStopC, kvStorage, raftConfig, localIAddr, monitorKV)
	ServeHttpKVAPI(kvStore, localIAddr, kvServiceStopC)
}
