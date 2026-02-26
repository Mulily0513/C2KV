package config

type RaftConfig struct {
	ElectionTick   int    `yaml:"electionTick"`
	HeartbeatTick  int    `yaml:"heartbeatTick"`
	RequestTimeOut int    `yaml:"requestTimeOut"`
	MaxInflightMsg int    `yaml:"maxInflightMsg" json:"maxInflightMsg"`
	MaxSizePerMsg  uint64 `yaml:"maxSizePerMsg" json:"maxSizePerMsg"`
	EAddr          string `yaml:"eAddr"`
	IAddr          string `yaml:"iAddr"`
	Peers          []Peer `yaml:"peers"`
}

type Peer struct {
	Name  string `yaml:"name"`
	Id    uint64 `yaml:"id"`
	IAddr string `yaml:"iAddr"`
	EAddr string `yaml:"eAddr"`
}

func (r *RaftConfig) GetPeerIds() (peerIds []uint64) {
	for _, peer := range r.Peers {
		peerIds = append(peerIds, peer.Id)
	}
	return
}

func (r *RaftConfig) NormalizedMaxInflightMsg() int {
	if r.MaxInflightMsg <= 0 {
		return 256
	}
	return r.MaxInflightMsg
}

func (r *RaftConfig) NormalizedMaxSizePerMsg() uint64 {
	if r.MaxSizePerMsg == 0 {
		return 1024 * 1024
	}
	return r.MaxSizePerMsg
}
