package config

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"strings"
)

var (
	vip  = viper.New()
	conf = new(config)
)

type config struct {
	zapConf  *ZapConfig
	dbConf   *DbConfig
	raftConf *RaftConfig
}

func InitConfig(cfgPath string) {
	var err error
	vip.SetConfigFile(cfgPath)
	vip.SetConfigType("yaml")
	if err = viper.ReadInConfig(); err != nil {
		panic(err)
	}
	if err = viper.Unmarshal(cfgPath); err != nil {
		log.Errorf("", err)
	}
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		if err = viper.Unmarshal(&config{}); err != nil {
		}
	})
}

func GetZapConf() *ZapConfig {
	return conf.zapConf
}

func GetRaftConf() *RaftConfig {
	return conf.raftConf
}

func GetDBConf() *DbConfig {
	return conf.dbConf
}

func GetLocalInfo() (localIAddr string, localEAddr string, localId uint64, peers []Peer) {
	raftConf := conf.raftConf
	for _, peer := range raftConf.Peers {
		if strings.Split(peer.EAddr, ":")[0] == strings.Split(peer.IAddr, ":")[0] {
			localId = peer.Id
			localIAddr = peer.IAddr
			localIAddr = peer.EAddr
		}
		peers = append(peers, peer)
	}
	return
}
