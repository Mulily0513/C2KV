package config

import (
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"strings"
)

var conf config

type config struct {
	ZapConfig  *ZapConfig  `yaml:"zapConfig"  json:"zapConfig"`
	DbConfig   *DBConfig   `yaml:"dbConfig"   json:"dbConfig"`
	RaftConfig *RaftConfig `yaml:"raftConfig" json:"raftConfig"`
}

func InitConfig(cfgPath string) {
	var err error
	viper.SetConfigFile(cfgPath)
	viper.SetConfigType("json")
	if err = viper.ReadInConfig(); err != nil {
		panic(err)
	}
	if err = viper.Unmarshal(&conf); err != nil {
		panic(err)
	}
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		if err = viper.Unmarshal(conf); err != nil {
		}
	})
}

func GetZapConf() *ZapConfig {
	return conf.ZapConfig
}

func GetRaftConf() *RaftConfig {
	return conf.RaftConfig
}

func GetDBConf() *DBConfig {
	return conf.DbConfig
}

type LocalInfo struct {
	LocalId    uint64
	LocalIAddr string
	LocalEAddr string
	Peers      []Peer
}

func GetLocalInfo() (localInfo LocalInfo) {
	raftConf := conf.RaftConfig
	for _, peer := range raftConf.Peers {
		if strings.Split(raftConf.EAddr, ":")[0] == strings.Split(peer.IAddr, ":")[0] {
			localInfo.LocalId = peer.Id
			localInfo.LocalIAddr = peer.IAddr
		}
		localInfo.Peers = append(localInfo.Peers, peer)
	}
	return
}
