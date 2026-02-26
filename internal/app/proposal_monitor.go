package app

import "sync"

const proposalMonitorShards = 64

type proposalMonitor struct {
	shards     [proposalMonitorShards]proposalMonitorShard
	signalPool sync.Pool
}

type proposalMonitorShard struct {
	mu sync.Mutex
	m  map[uint64]chan struct{}
}

func newProposalMonitor() *proposalMonitor {
	pm := &proposalMonitor{}
	pm.signalPool.New = func() any {
		return make(chan struct{}, 1)
	}
	for i := range pm.shards {
		pm.shards[i].m = make(map[uint64]chan struct{})
	}
	return pm
}

func (pm *proposalMonitor) acquireSignal() chan struct{} {
	v := pm.signalPool.Get()
	if v == nil {
		return make(chan struct{}, 1)
	}
	sig, ok := v.(chan struct{})
	if !ok || sig == nil {
		return make(chan struct{}, 1)
	}
	return sig
}

func (pm *proposalMonitor) releaseSignal(sig chan struct{}) {
	if sig == nil {
		return
	}
	for {
		select {
		case <-sig:
		default:
			pm.signalPool.Put(sig)
			return
		}
	}
}

func (pm *proposalMonitor) shard(id uint64) *proposalMonitorShard {
	return &pm.shards[id&(proposalMonitorShards-1)]
}

func (pm *proposalMonitor) put(id uint64, sig chan struct{}) {
	sh := pm.shard(id)
	sh.mu.Lock()
	sh.m[id] = sig
	sh.mu.Unlock()
}

func (pm *proposalMonitor) delete(id uint64) {
	sh := pm.shard(id)
	sh.mu.Lock()
	delete(sh.m, id)
	sh.mu.Unlock()
}

func (pm *proposalMonitor) pop(id uint64) (chan struct{}, bool) {
	sh := pm.shard(id)
	sh.mu.Lock()
	sig, ok := sh.m[id]
	if ok {
		delete(sh.m, id)
	}
	sh.mu.Unlock()
	return sig, ok
}

func (pm *proposalMonitor) popBatch(ids []uint64) []chan struct{} {
	if len(ids) == 0 {
		return nil
	}

	positionsByShard := make([][]int, proposalMonitorShards)
	for pos, id := range ids {
		sh := id & (proposalMonitorShards - 1)
		positionsByShard[sh] = append(positionsByShard[sh], pos)
	}

	slots := make([]chan struct{}, len(ids))
	for shardID, positions := range positionsByShard {
		if len(positions) == 0 {
			continue
		}
		sh := &pm.shards[shardID]
		sh.mu.Lock()
		for _, pos := range positions {
			id := ids[pos]
			if sig, ok := sh.m[id]; ok {
				slots[pos] = sig
				delete(sh.m, id)
			}
		}
		sh.mu.Unlock()
	}

	sigs := make([]chan struct{}, 0, len(ids))
	for _, sig := range slots {
		if sig != nil {
			sigs = append(sigs, sig)
		}
	}
	return sigs
}

func (pm *proposalMonitor) notifyBatch(ids []uint64) {
	if len(ids) == 0 {
		return
	}

	var positionsByShard [proposalMonitorShards][]int
	for pos, id := range ids {
		sh := id & (proposalMonitorShards - 1)
		positionsByShard[sh] = append(positionsByShard[sh], pos)
	}

	for shardID, positions := range positionsByShard {
		if len(positions) == 0 {
			continue
		}
		sh := &pm.shards[shardID]
		sh.mu.Lock()
		for _, pos := range positions {
			id := ids[pos]
			sig, ok := sh.m[id]
			if !ok {
				continue
			}
			delete(sh.m, id)
			select {
			case sig <- struct{}{}:
			default:
			}
		}
		sh.mu.Unlock()
	}
}

func (pm *proposalMonitor) len() int {
	total := 0
	for i := range pm.shards {
		sh := &pm.shards[i]
		sh.mu.Lock()
		total += len(sh.m)
		sh.mu.Unlock()
	}
	return total
}
