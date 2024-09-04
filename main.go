package main

import (
	"flag"
	"github.com/Mulily0513/C2KV/app"
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/log"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "cfg", "./bin/config_debug.json", "c2kv config path")
	flag.Parse()
	config.InitConfig(cfgPath)
	log.InitLog(config.GetZapConf())
	app.StartAppNode(config.GetLocalInfo(), db.OpenKVStorage(config.GetDBConf()), config.GetRaftConf())
}
