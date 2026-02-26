package app

import (
	"encoding/binary"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	config "github.com/Mulily0513/C2KV/internal/config"
	"github.com/Mulily0513/C2KV/internal/db"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/log"
	raft "github.com/Mulily0513/C2KV/internal/raft"
	transport "github.com/Mulily0513/C2KV/internal/transport"
	"github.com/Mulily0513/C2KV/internal/transport/types"
	"net"
	"sync"
	"time"
)

const (
	proposeChanBufferSize = 16384
	proposeDrainBatchSize = 1024
	applyChanBufferSize   = 512
	applyDrainBatchSize   = 16
)

type AppNode struct {
	localId    uint64
	localIAddr string
	peers      []config.Peer
	monitor    *proposalMonitor

	raftNode  raft.Node
	transport transport.Transporter
	kvStorage db.Storage

	proposeC       chan []byte
	confChangeC    chan pb.ConfChange
	kvServiceStopC chan struct{}
	applyC         chan []*pb.Entry
	applyBatchPool sync.Pool
	applyKVPool    sync.Pool
	applyKVPtrPool sync.Pool
	applyIDPool    sync.Pool
}

type readySyncStorage interface {
	BeginReadySync(mustSync bool)
	SyncReadyBeforeSend() error
	FinishReadySync() error
}

func StartAppNode(localInfo config.LocalInfo, kvStorage db.Storage, raftConfig *config.RaftConfig) {
	log.Infof("start app node, local info : %+v", localInfo)
	proposeC := make(chan []byte, proposeChanBufferSize)
	confChangeC := make(chan pb.ConfChange)
	kvServiceStopC := make(chan struct{})
	applyC := make(chan []*pb.Entry, applyChanBufferSize)
	monitor := newProposalMonitor()

	an := &AppNode{
		localId:        localInfo.LocalId,
		localIAddr:     localInfo.LocalIAddr,
		peers:          localInfo.Peers,
		proposeC:       proposeC,
		confChangeC:    confChangeC,
		kvServiceStopC: kvServiceStopC,
		applyC:         applyC,
		kvStorage:      kvStorage,
		monitor:        monitor,
	}
	an.applyBatchPool.New = func() any {
		return make([]*pb.Entry, 0, applyDrainBatchSize)
	}
	an.applyKVPool.New = func() any {
		return make([]marshal.KV, 0, applyDrainBatchSize*64)
	}
	an.applyKVPtrPool.New = func() any {
		return make([]*marshal.KV, 0, applyDrainBatchSize*64)
	}
	an.applyIDPool.New = func() any {
		return make([]uint64, 0, applyDrainBatchSize*64)
	}

	// Complete the network connection between the current node and other nodes in the cluster.
	an.servePeerRaft()

	// Start the Raft algorithm layer.
	an.raftNode = raft.StartRaftNode(localInfo.LocalId, raftConfig, kvStorage)
	// Start a goroutine to handle the interaction between appNode and raftNode.
	go an.serveRaftNode()
	// Start a goroutine to handle node changes and log proposals for client requests.
	go an.servePropCAndConfC()
	// Start apply worker so raft ready loop can pipeline advance with apply.
	go an.serveApplyC()

	StartKVAPIService(proposeC, raftConfig.RequestTimeOut, kvStorage, monitor, localInfo.LocalEAddr, kvServiceStopC, an.raftNode)
	return
}

func (an *AppNode) servePeerRaft() {
	an.transport = &transport.Transport{
		LocalId:      types.ID(an.localId),
		LocalIAddr:   an.localIAddr,
		RaftOperator: an,
		Peers:        make(map[types.ID]transport.Peer),
		StopC:        make(chan struct{}),
	}

	ln, err := net.Listen("tcp", an.localIAddr)
	if err != nil {
		log.Panicf("start listening failed %s", an.localIAddr)
	}
	log.Infof("node(id:%d iaddr:%s) start listening......", an.localId, an.localIAddr)

	for _, peer := range an.peers {
		if peer.Id != an.localId {
			an.transport.AddPeer(types.ID(peer.Id), peer.IAddr)
		}
	}

	go an.transport.ListenPeer(ln)
}

func (an *AppNode) serveRaftNode() {
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				an.raftNode.Tick()
			case <-stopTick:
				return
			}
		}
	}()

	for {
		rd := <-an.raftNode.Ready()
		var rs readySyncStorage
		if readyAware, ok := an.kvStorage.(readySyncStorage); ok {
			rs = readyAware
			rs.BeginReadySync(rd.MustSync)
		}
		hasHardState := !raft.IsEmptyHardState(rd.HardState)
		hasUnstable := len(rd.UnstableEntries) > 0
		switch {
		case hasHardState && hasUnstable:
			log.Debugf("ready, persist hard state: %+v", rd.HardState)
			log.Debugf("ready, persist unstable entries: %+v", rd.UnstableEntries)
			var wg sync.WaitGroup
			wg.Add(2)
			an.kvStorage.PersistHardState(rd.HardState, &wg)
			an.kvStorage.PersistUnstableEnts(rd.UnstableEntries, &wg)
			wg.Wait()
		case hasHardState:
			log.Debugf("ready, persist hard state: %+v", rd.HardState)
			var wg sync.WaitGroup
			wg.Add(1)
			an.kvStorage.PersistHardState(rd.HardState, &wg)
			wg.Wait()
		case hasUnstable:
			log.Debugf("ready, persist unstable entries: %+v", rd.UnstableEntries)
			var wg sync.WaitGroup
			wg.Add(1)
			an.kvStorage.PersistUnstableEnts(rd.UnstableEntries, &wg)
			wg.Wait()
		}

		// Keep follower path conservative (sync -> send), but pipeline leader path
		// as send -> sync to overlap local durability with follower replication.
		isLeader := an.raftNode.Status().IsLeader
		if len(rd.Messages) > 0 && isLeader {
			msgDebugExceptHeartbeat(rd.Messages)
			an.transport.Send(rd.Messages)
		}

		if rs != nil {
			if err := rs.SyncReadyBeforeSend(); err != nil {
				log.Panicf("sync ready before send failed: %v", err)
			}
		}

		if len(rd.Messages) > 0 && !isLeader {
			msgDebugExceptHeartbeat(rd.Messages)
			an.transport.Send(rd.Messages)
		}

		if len(rd.CommittedEntries) > 0 {
			log.Debugf("ready, apply committed entries: %+v", rd.CommittedEntries)
			an.enqueueCommittedEnts(rd.CommittedEntries)
		}

		if rs != nil {
			if err := rs.FinishReadySync(); err != nil {
				log.Panicf("finish ready sync failed: %v", err)
			}
		}

		an.raftNode.Advance()
	}
}

func (an *AppNode) enqueueCommittedEnts(ents []*pb.Entry) {
	if len(ents) == 0 {
		return
	}
	// Copy into pooled buffer so the next ready assignment won't affect this batch.
	batch := an.getApplyBatch(len(ents))
	batch = append(batch, ents...)
	an.applyC <- batch
}

func (an *AppNode) serveApplyC() {
	for {
		ents := <-an.applyC
		if len(ents) == 0 {
			an.putApplyBatch(ents)
			continue
		}
		merged := ents
		recycle := [][]*pb.Entry{ents}
		for i := 1; i < applyDrainBatchSize; i++ {
			select {
			case next := <-an.applyC:
				if len(next) == 0 {
					an.putApplyBatch(next)
					continue
				}
				recycle = append(recycle, next)
				merged = append(merged, next...)
			default:
				i = applyDrainBatchSize
			}
		}
		an.applyCommittedEnts(merged)
		for _, batch := range recycle {
			an.putApplyBatch(batch)
		}
	}
}

func (an *AppNode) getApplyBatch(sizeHint int) []*pb.Entry {
	v := an.applyBatchPool.Get()
	if v == nil {
		return make([]*pb.Entry, 0, sizeHint)
	}
	batch, _ := v.([]*pb.Entry)
	if cap(batch) < sizeHint {
		return make([]*pb.Entry, 0, sizeHint)
	}
	return batch[:0]
}

func (an *AppNode) putApplyBatch(batch []*pb.Entry) {
	if batch == nil {
		return
	}
	an.applyBatchPool.Put(batch[:0])
}

func (an *AppNode) getApplyKVVals(sizeHint int) []marshal.KV {
	v := an.applyKVPool.Get()
	if v == nil {
		return make([]marshal.KV, 0, sizeHint)
	}
	kvs, _ := v.([]marshal.KV)
	if cap(kvs) < sizeHint {
		return make([]marshal.KV, 0, sizeHint)
	}
	return kvs[:0]
}

func (an *AppNode) putApplyKVVals(kvs []marshal.KV) {
	if kvs == nil {
		return
	}
	an.applyKVPool.Put(kvs[:0])
}

func (an *AppNode) getApplyKVPtrs(sizeHint int) []*marshal.KV {
	v := an.applyKVPtrPool.Get()
	if v == nil {
		return make([]*marshal.KV, 0, sizeHint)
	}
	kvs, _ := v.([]*marshal.KV)
	if cap(kvs) < sizeHint {
		return make([]*marshal.KV, 0, sizeHint)
	}
	return kvs[:0]
}

func (an *AppNode) putApplyKVPtrs(kvs []*marshal.KV) {
	if kvs == nil {
		return
	}
	an.applyKVPtrPool.Put(kvs[:0])
}

func (an *AppNode) getApplyIDs(sizeHint int) []uint64 {
	v := an.applyIDPool.Get()
	if v == nil {
		return make([]uint64, 0, sizeHint)
	}
	ids, _ := v.([]uint64)
	if cap(ids) < sizeHint {
		return make([]uint64, 0, sizeHint)
	}
	return ids[:0]
}

func (an *AppNode) putApplyIDs(ids []uint64) {
	if ids == nil {
		return
	}
	an.applyIDPool.Put(ids[:0])
}

func msgDebugExceptHeartbeat(msgs []*pb.Message) {
	if !log.DebugEnabled() {
		return
	}
	for _, m := range msgs {
		if m.Type != pb.MsgHeartbeat && m.Type != pb.MsgHeartbeatResp {
			log.Debugf("ready, send peer msg: %+v", *m)
		}
	}
}

func (an *AppNode) servePropCAndConfC() {
	for an.proposeC != nil {
		prop, ok := <-an.proposeC
		if !ok {
			an.proposeC = nil
			break
		}
		if len(prop) == 0 {
			continue
		}

		entries := make([]pb.Entry, 0, proposeDrainBatchSize)
		entries = append(entries, pb.Entry{Data: prop})
		for i := 1; i < proposeDrainBatchSize; i++ {
			select {
			case prop, ok = <-an.proposeC:
				if !ok {
					an.proposeC = nil
					i = proposeDrainBatchSize
					continue
				}
				if len(prop) == 0 {
					continue
				}
				entries = append(entries, pb.Entry{Data: prop})
			default:
				i = proposeDrainBatchSize
			}
		}
		if len(entries) == 0 {
			continue
		}
		an.raftNode.Step(&pb.Message{Type: pb.MsgProp, Entries: entries})
	}
}

func (an *AppNode) applyCommittedEnts(ents []*pb.Entry) {
	kvVals := an.getApplyKVVals(len(ents))
	kvs := an.getApplyKVPtrs(len(ents))
	applyIDs := an.getApplyIDs(len(ents))
	for _, entry := range ents {
		if entry.Type != pb.EntryNormal {
			continue
		}
		if len(entry.Data) < marshal.ApplySigSize+marshal.KeySize {
			log.Panicf("malformed raft entry payload too short, index=%d len=%d", entry.Index, len(entry.Data))
		}
		if id := binary.LittleEndian.Uint64(entry.Data[:8]); id != 0 {
			applyIDs = append(applyIDs, id)
		}
		keySize := int(binary.LittleEndian.Uint32(entry.Data[marshal.ApplySigSize : marshal.ApplySigSize+marshal.KeySize]))
		keyStart := marshal.ApplySigSize + marshal.KeySize
		keyEnd := keyStart + keySize
		if keySize < 0 || keyEnd > len(entry.Data) {
			log.Panicf("malformed raft entry key range, index=%d keySize=%d payloadLen=%d", entry.Index, keySize, len(entry.Data))
		}
		dataBytes := entry.Data[keyEnd:]
		if len(dataBytes) < marshal.IndexSize {
			log.Panicf("malformed raft entry data payload, index=%d dataLen=%d", entry.Index, len(dataBytes))
		}
		// Reuse entry payload slice; memtable Put copies bytes into arena.
		binary.LittleEndian.PutUint64(dataBytes[:marshal.IndexSize], entry.Index)
		kvVals = append(kvVals, marshal.KV{
			KeySize:   uint32(keySize),
			Key:       entry.Data[keyStart:keyEnd],
			DataBytes: dataBytes,
		})
		kvs = append(kvs, &kvVals[len(kvVals)-1])
	}
	if len(kvs) == 0 {
		an.putApplyKVVals(kvVals)
		an.putApplyKVPtrs(kvs)
		an.putApplyIDs(applyIDs)
		return
	}

	if err := an.kvStorage.Apply(kvs); err != nil {
		log.Panicf("apply ents failed %v", err)
		an.putApplyKVVals(kvVals)
		an.putApplyKVPtrs(kvs)
		an.putApplyIDs(applyIDs)
		return
	}

	an.monitor.notifyBatch(applyIDs)
	an.putApplyKVVals(kvVals)
	an.putApplyKVPtrs(kvs)
	an.putApplyIDs(applyIDs)
	return
}

// Process Rat network layer interface. The network layer interacts with the RaftNode through this interface.
func (an *AppNode) Process(m *pb.Message) {
	an.raftNode.Step(m)
}

func (an *AppNode) ReportUnreachable(id uint64) {
	an.raftNode.ReportUnreachable(id)
}

func (an *AppNode) Close() {
	an.transport.Stop()
	an.raftNode.Stop()
	close(an.proposeC)
	close(an.confChangeC)
	close(an.kvServiceStopC)
}
