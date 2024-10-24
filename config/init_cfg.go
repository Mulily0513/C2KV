package config

import (
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
)

var conf config
var Mode string

type config struct {
	ZapConfig  *ZapConfig  `yaml:"zapConfig"  json:"zapConfig"`
	DbConfig   *DBConfig   `yaml:"dbConfig"   json:"dbConfig"`
	RaftConfig *RaftConfig `yaml:"raftConfig" json:"raftConfig"`
}

func InitConfig(cfgPath string) {
	var err error
	viper.SetConfigFile(cfgPath)
	viper.SetConfigType("yaml")
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
	LocalName  string
	LocalIAddr string
	LocalEAddr string
	Peers      []Peer
}

func GetLocalInfo() (localInfo LocalInfo) {
	if Mode == "debug" {
		localInfo.LocalId, _ = strconv.ParseUint(os.Getenv("C2KV_NODE_ID"), 10, 64)
		localInfo.LocalName = os.Getenv("C2KV_NODE_NAME")
		localInfo.LocalIAddr = os.Getenv("C2KV_NODE_IA")
		localInfo.LocalEAddr = os.Getenv("C2KV_NODE_EA")
		nodePeers := os.Getenv("C2KV_NODE_PEERS")
		peers := strings.Split(nodePeers, ",")
		for _, peer := range peers {
			items := strings.Split(peer, "-")
			id, _ := strconv.ParseUint(items[0], 10, 64)
			p := Peer{
				Id:    id,
				Name:  items[1],
				IAddr: items[2],
			}
			localInfo.Peers = append(localInfo.Peers, p)
		}
	} else {
		raftConf := conf.RaftConfig
		for _, peer := range raftConf.Peers {
			if strings.Split(raftConf.EAddr, ":")[0] == strings.Split(peer.IAddr, ":")[0] {
				localInfo.LocalId = peer.Id
				localInfo.LocalIAddr = peer.IAddr
			}
			localInfo.Peers = append(localInfo.Peers, peer)
		}
	}
	return
}
