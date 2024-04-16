package config

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"strings"
)

var (
	Viper *viper.Viper
	Conf  *Config
)

type Config struct {
	ZapConf    *ZapConfig
	DBConfig   *DBConfig
	RaftConfig *RaftConfig
}

func InitConfig() {
	defaultConfigPath := "/Users/hlhf/GolandProjects/Cold2DB/bin/config.yaml"
	Viper = viper.New()
	Viper.SetConfigFile(defaultConfigPath)
	Viper.SetConfigType("yaml")
	err := Viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	Conf = new(Config)
	if err = Viper.Unmarshal(Conf); err != nil {
		log.Errorf("", err)
	}
	Viper.WatchConfig()
	Viper.OnConfigChange(func(e fsnotify.Event) {
		if err = Viper.Unmarshal(&Config{}); err != nil {
		}
	})
}

func GetZapConf() *ZapConfig {
	return Conf.ZapConf
}

func GetRaftConf() *RaftConfig {
	return Conf.RaftConfig
}

func GetDBConf() *DBConfig {
	return Conf.DBConfig
}

func GetLocalInfo() (localIpAddr string, localId uint64, peers []Peer) {
	raftConf := Conf.RaftConfig
	for _, node := range raftConf.Peers {
		if strings.Contains(node.EAddr, "127.0.0.1") && strings.Contains(node.IAddr, "127.0.0.1") {
			localId = node.Id
			localIpAddr = node.EAddr
		}
		peers = append(peers, node)
	}
	return
}
