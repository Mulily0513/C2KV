package config

type RaftConfig struct {
	ElectionTick   int    `yaml:"electionTick"`
	HeartbeatTick  int    `yaml:"heartbeatTick"`
	RequestTimeOut int    `yaml:"requestTimeOut"`
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
