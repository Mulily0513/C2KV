package config

// RaftConfig 表示Raft配置的结构体
type RaftConfig struct {
	ElectionTick   int    `yaml:"electionTick"`
	HeartbeatTick  int    `yaml:"heartbeatTick"`
	RequestTimeOut int    `yaml:"requestTimeOut"`
	EAddr          string `yaml:"eAddr"`
	Peers          []Peer `yaml:"peers"`
}

// Peer 表示Raft节点的结构体
type Peer struct {
	Name  string `yaml:"name"`
	Id    uint64 `yaml:"id"`
	IAddr string `yaml:"iAddr"`
}
