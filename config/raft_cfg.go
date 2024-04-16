package config

type RaftConfig struct {
	Id uint64
	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick  int
	RequestLimit   int
	RequestTimeout int
	Peers          []Peer `yaml:"peers"`
}

type Peer struct {
	Id    uint64 `yaml:"id"`
	EAddr string `yaml:"eAddr"`
	IAddr string `yaml:"iAddr"`
}
