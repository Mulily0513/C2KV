package main

import (
	"github.com/Mulily0513/C2KV/config"
	"github.com/Mulily0513/C2KV/db"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/log"
	"github.com/Mulily0513/C2KV/pb"
	"github.com/Mulily0513/C2KV/raft"
	"github.com/Mulily0513/C2KV/transport"
	"github.com/Mulily0513/C2KV/transport/types"
	"time"
)

type AppNode struct {
	localId    uint64
	localIAddr string
	localEAddr string
	peers      []config.Peer
	monitorKV  map[int64]chan struct{}

	raftNode  raft.Node
	transport transport.Transporter
	kvStorage db.Storage

	proposeC    chan []byte        // 提议 (k,v) channel
	confChangeC chan pb.ConfChange // 提议更改配置文件 channel
	kvServStopC chan struct{}      // 关闭http服务器的信号 channel
}

func StartAppNode(localId uint64, localIAddr string, peers []config.Peer, proposeC chan []byte, confChangeC chan pb.ConfChange,
	kvHTTPStopC chan struct{}, kvStorage db.Storage, raftConfig *config.RaftConfig, monitorKV map[int64]chan struct{}) {
	an := &AppNode{
		localId:     localId,
		localIAddr:  localIAddr,
		peers:       peers,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		kvServStopC: kvHTTPStopC,
		kvStorage:   kvStorage,
		monitorKV:   monitorKV,
	}

	// 完成当前节点与集群中其他节点之间的网络连接
	an.servePeerRaft()
	// 启动Raft
	an.raftNode = raft.StartRaftNode(raftConfig, kvStorage)
	// 启动一个goroutine,处理appNode与raftNode的交互
	go an.serveRaftNode()
	// 启动一个goroutine,处理客户端请求的节点变更以及日志提议
	go an.servePropCAndConfC()

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

	go an.transport.ListenPeer(an.localIAddr)

	for _, peer := range an.peers {
		an.transport.AddPeer(types.ID(peer.Id), peer.IAddr)
	}
}

func (an *AppNode) serveRaftNode() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()
		case rd := <-an.raftNode.Ready():
			log.Debug("start handle ready").Record()
			//todo 并发？
			if !raft.IsEmptyHardState(rd.HardState) {
				if err = an.kvStorage.PersistHardState(rd.HardState, rd.ConfState); err != nil {
					log.Panicf("save hard state failed", err)
				}
			}

			if len(rd.UnstableEntries) > 0 {
				if err = an.kvStorage.PersistUnstableEnts(rd.UnstableEntries); err != nil {
					log.Panicf("save entries failed", err)
				}
			}

			if len(rd.CommittedEntries) > 0 {
				if err = an.applyCommittedEnts(rd.CommittedEntries); err != nil {
					log.Panicf("apply entries failed", err)
				}
			}

			if len(rd.Messages) > 0 {
				go an.transport.Send(rd.Messages)
			}

			//通知raftNode本轮ready已经处理完可以进行下一轮处理
			an.raftNode.Advance()
			log.Debug("handle ready success").Record()
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

func (an *AppNode) applyCommittedEnts(ents []*pb.Entry) (err error) {
	entries := make([]*pb.Entry, 0)

	//apply entries
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
	kvIds := make([]int64, 0)
	for _, entry := range entries {
		kv = marshal.DecodeKV(entry.Data)
		kvs = append(kvs, kv)
		kvIds = append(kvIds, kv.ApplySig)
	}

	if err = an.kvStorage.Apply(kvs); err != nil {
		return
	}

	for _, id := range kvIds {
		close(an.monitorKV[id])
		delete(an.monitorKV, id)
	}

	return
}

// Process Rat网络层接口,网络层通过该接口与RaftNode交互
func (an *AppNode) Process(m *pb.Message) {
	an.raftNode.Step(m)
}

func (an *AppNode) ReportUnreachable(id uint64) { an.raftNode.ReportUnreachable(id) }

// 关闭Raft
func (an *AppNode) stop() {
	an.transport.Stop()
	an.raftNode.Stop()
	close(an.proposeC)
	close(an.confChangeC)
	close(an.kvServStopC)
}
