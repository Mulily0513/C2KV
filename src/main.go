package main

import (
	"flag"
	"github.com/Mulily0513/C2KV/src/app"
	"github.com/Mulily0513/C2KV/src/config"
	"github.com/Mulily0513/C2KV/src/db"
	"github.com/Mulily0513/C2KV/src/log"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "c", "./deploy/dev/config_dev.yaml", "c2kv config path")
	flag.StringVar(&config.Mode, "m", "release", "c2kv mode")
	flag.Parse()
	config.InitConfig(cfgPath)
	log.InitLog(config.GetZapConf())
	app.StartAppNode(config.GetLocalInfo(), db.OpenKVStorage(config.GetDBConf()), config.GetRaftConf())
}
