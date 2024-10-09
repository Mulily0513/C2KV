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

	proposeC       chan []byte        // 提议 (k,v) channel
	confChangeC    chan pb.ConfChange // 提议更改配置文件 channel
	kvServiceStopC chan struct{}      // 关闭http服务器的信号 channel
}

func StartAppNode(localInfo config.LocalInfo, kvStorage db.Storage, raftConfig *config.RaftConfig) {
	proposeC := make(chan []byte, raftConfig.RequestTimeOut)
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

	// 完成当前节点与集群中其他节点之间的网络连接
	an.servePeerRaft()

	// 启动Raft算法层
	an.raftNode = raft.StartRaftNode(localInfo.LocalId, raftConfig, kvStorage)
	// 启动一个goroutine,处理appNode与raftNode的交互
	go an.serveRaftNode()
	// 启动一个goroutine,处理客户端请求的节点变更以及日志提议
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

	log.Infof("node(id:%d)start listening, local addr : %s", an.localId, an.localIAddr)
	ln, err := net.Listen("tcp", an.localIAddr)
	if err != nil {
		log.Panicf("start listening failed %s", an.localIAddr)
	}

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
			log.Debugf("ready:%+v", rd)
			if !raft.IsEmptyHardState(rd.HardState) {
				wg.Add(1)
				go an.kvStorage.PersistHardState(rd.HardState, wg)
			}

			if len(rd.UnstableEntries) > 0 {
				wg.Add(1)
				go an.kvStorage.PersistUnstableEnts(rd.UnstableEntries, wg)
			}

			if len(rd.CommittedEntries) > 0 {
				wg.Add(1)
				go an.applyCommittedEnts(rd.CommittedEntries, wg)
			}

			if len(rd.Messages) > 0 {
				go an.transport.Send(rd.Messages)
			}

			wg.Wait()
			an.raftNode.Advance()
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
			if len(ents[i].Data) == 0 {
				continue
			}
			entries = append(entries, entry)
		}
	}

	var kv *marshal.KV
	kvs := make([]*marshal.KV, len(entries))
	kvUUIDs := make([][]byte, 0)
	for _, entry := range entries {
		kv = marshal.DecodeKV(entry.Data)
		kvs = append(kvs, kv)
		kvUUIDs = append(kvUUIDs, kv.ApplySig)
	}

	if err := an.kvStorage.Apply(kvs); err != nil {
		log.Panicf("apply ents")
		return
	}

	for _, id := range kvUUIDs {
		close(an.monitorKV[string(id)])
		delete(an.monitorKV, string(jd))
	}

	return
}

// Process Rat网络层接口,网络层通过该接口与RaftNode交互
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
