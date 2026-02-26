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
	if nodeID := strings.TrimSpace(os.Getenv("C2KV_NODE_ID")); nodeID != "" {
		localInfo.LocalId, _ = strconv.ParseUint(nodeID, 10, 64)
		localInfo.LocalName = os.Getenv("C2KV_NODE_NAME")
		localInfo.LocalIAddr = os.Getenv("C2KV_NODE_IA")
		localInfo.LocalEAddr = os.Getenv("C2KV_NODE_EA")
		nodePeers := os.Getenv("C2KV_NODE_PEERS")
		peers := strings.Split(nodePeers, ",")
		for _, peer := range peers {
			if strings.TrimSpace(peer) == "" {
				continue
			}
			items := strings.Split(peer, "-")
			if len(items) < 3 {
				continue
			}
			id, _ := strconv.ParseUint(items[0], 10, 64)
			p := Peer{
				Id:    id,
				Name:  items[1],
				IAddr: items[2],
			}
			localInfo.Peers = append(localInfo.Peers, p)
		}
		if len(localInfo.Peers) == 0 && conf.RaftConfig != nil {
			localInfo.Peers = append(localInfo.Peers, conf.RaftConfig.Peers...)
		}
	} else {
		raftConf := conf.RaftConfig
		raftHost := strings.Split(raftConf.EAddr, ":")[0]
		for _, peer := range raftConf.Peers {
			if raftHost == strings.Split(peer.EAddr, ":")[0] {
				localInfo.LocalId = peer.Id
				localInfo.LocalName = peer.Name
				localInfo.LocalIAddr = peer.IAddr
				localInfo.LocalEAddr = peer.EAddr
			}
			localInfo.Peers = append(localInfo.Peers, peer)
		}
	}
	return
}
