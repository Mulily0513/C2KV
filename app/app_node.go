package app

import (
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/raft"
	"github.com/Mulily0513/C2KV/transport"
	"github.com/Mulily0513/C2KV/transport/types"
	"net"
	"sync"
	"time"
)

type AppNode struct {
	localId    uint64
	localIAddr string
	peers      []config.Peer
	monitorKV  map[string]chan struct{}

	raftNode  raft.Node
	transport transport.Transporter
	kvStorage db.Storage

	proposeC       chan []byte
	confChangeC    chan pb.ConfChange
	kvServiceStopC chan struct{}
}

func StartAppNode(localInfo config.LocalInfo, kvStorage db.Storage, raftConfig *config.RaftConfig) {
	log.Infof("start app node, local info : %+v", localInfo)
	proposeC := make(chan []byte)
	confChangeC := make(chan pb.ConfChange)
	kvServiceStopC := make(chan struct{})
	monitorKV := make(map[string]chan struct{})

	an := &AppNode{
		localId:        localInfo.LocalId,
		localIAddr:     localInfo.LocalIAddr,
		peers:          localInfo.Peers,
		proposeC:       proposeC,
		confChangeC:    confChangeC,
		kvServiceStopC: kvServiceStopC,
		kvStorage:      kvStorage,
		monitorKV:      monitorKV,
	}

	// Complete the network connection between the current node and other nodes in the cluster.
	an.servePeerRaft()

	// Start the Raft algorithm layer.
	an.raftNode = raft.StartRaftNode(localInfo.LocalId, raftConfig, kvStorage)
	// Start a goroutine to handle the interaction between appNode and raftNode.
	go an.serveRaftNode()
	// Start a goroutine to handle node changes and log proposals for client requests.
	go an.servePropCAndConfC()

	StartKVAPIService(proposeC, raftConfig.RequestTimeOut, kvStorage, monitorKV, localInfo.LocalEAddr, kvServiceStopC, an.raftNode)
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
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()
		case rd := <-an.raftNode.Ready():
			wg := new(sync.WaitGroup)
			if !raft.IsEmptyHardState(rd.HardState) {
				log.Debugf("ready, persist hard state: %+v", rd.HardState)
				wg.Add(1)
				go an.kvStorage.PersistHardState(rd.HardState, wg)
			}

			if len(rd.UnstableEntries) > 0 {
				log.Debugf("ready, persist unstable entries: %+v", rd.UnstableEntries)
				wg.Add(1)
				go an.kvStorage.PersistUnstableEnts(rd.UnstableEntries, wg)
			}

			if len(rd.CommittedEntries) > 0 {
				log.Debugf("ready, apply committed entries: %+v", rd.CommittedEntries)
				wg.Add(1)
				go an.applyCommittedEnts(rd.CommittedEntries, wg)
			}

			if len(rd.Messages) > 0 {
				msgDebugExceptHeartbeat(rd.Messages)
				go an.transport.Send(rd.Messages)
			}

			wg.Wait()
			an.raftNode.Advance()
		}
	}
}

func msgDebugExceptHeartbeat(msgs []*pb.Message) {
	for _, m := range msgs {
		if m.Type != pb.MsgHeartbeat && m.Type != pb.MsgHeartbeatResp {
			log.Debugf("ready, send peer msg: %+v", *m)
		}
	}
}

func (an *AppNode) servePropCAndConfC() {
	for an.proposeC != nil {
		select {
		case prop := <-an.proposeC:
			an.raftNode.Propose(prop)
		}
	}
}

func (an *AppNode) applyCommittedEnts(ents []*pb.Entry, wg *sync.WaitGroup) {
	defer wg.Done()

	entries := make([]*pb.Entry, 0)
	for i, entry := range ents {
		switch ents[i].Type {
		case pb.EntryNormal:
			entries = append(entries, entry)
		}
	}

	kvs := make([]*marshal.KV, 0)
	kvUUIDs := make([][]byte, 0)
	for _, entry := range entries {
		kv := marshal.DecodeKV(entry.Data)
		kv.Data.Index = entry.Index
		kvs = append(kvs, kv)
		kvUUIDs = append(kvUUIDs, kv.ApplySig)
	}

	if err := an.kvStorage.Apply(kvs); err != nil {
		log.Panicf("apply ents failed %v", err)
		return
	}

	if len(an.monitorKV) > 0 {
		for _, id := range kvUUIDs {
			close(an.monitorKV[string(id)])
			delete(an.monitorKV, string(id))
		}
	}
	return
}

// Process Rat network layer interface. The network layer interacts with the RaftNode through this interface.
func (an *AppNode) Process(m *pb.Message) {
	an.raftNode.Step(m)
}

func (an *AppNode) Close() {
	an.transport.Stop()
	an.raftNode.Stop()
	close(an.proposeC)
	close(an.confChangeC)
	close(an.kvServiceStopC)
}
