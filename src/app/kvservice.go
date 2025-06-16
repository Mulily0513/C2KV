package app

import (
	"errors"
	"github.com/Mulily0513/C2KV/c2kvserverpb"
	"github.com/Mulily0513/C2KV/src/db"
	"github.com/Mulily0513/C2KV/src/db/marshal"
	"github.com/Mulily0513/C2KV/src/log"
	"github.com/Mulily0513/C2KV/src/raft"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"net"
	"time"
)

type KvService struct {
	Storage    db.Storage
	proposeC   chan<- []byte
	monitorKV  map[string]chan struct{}
	ReqTimeout time.Duration
	RaftNode   raft.Node
}

func StartKVAPIService(proposeC chan<- []byte, requestTimeOut int, kvStorage db.Storage,
	monitorKV map[string]chan struct{}, localEAddr string, kvServiceStopC chan struct{}, raftNode raft.Node) {
	s := &KvService{
		Storage:    kvStorage,
		proposeC:   proposeC,
		monitorKV:  monitorKV,
		ReqTimeout: time.Duration(requestTimeOut) * time.Millisecond,
		RaftNode:   raftNode,
	}
	ServeGRPCKVAPI(s, localEAddr)
	<-kvServiceStopC
	return
}

func ServeGRPCKVAPI(kvService *KvService, localEAddr string) {
	listen, err := net.Listen("tcp", localEAddr)
	if err != nil {
		log.Panicf("failed listen grpc tcp, addr:%s", localEAddr)
	}

	server := grpc.NewServer()
	c2kvserverpb.RegisterKVServer(server, &KVService{kvService: kvService})
	c2kvserverpb.RegisterMaintenanceServer(server, &MaintainService{kvService: kvService})

	if err := server.Serve(listen); err != nil {
		log.Panicf("failed to serve grpc, addr:%s", localEAddr)
	}
}

func (s *KvService) Propose(key, val []byte, delete bool) error {
	timeOutC := time.NewTimer(s.ReqTimeout)
	uid := []byte(uuid.New().String())
	kv := new(marshal.KV)
	kv.Data = new(marshal.Data)
	kv.Key = key
	kv.Data.Value = val
	kv.Data.TimeStamp = time.Now().Unix()
	kv.ApplySig = uid
	if delete {
		kv.Data.Type = marshal.TypeDelete
	} else {
		kv.Data.Type = marshal.TypeInsert
	}
	buf := marshal.EncodeKV(kv)
	s.proposeC <- buf

	// monitor this key-value. When this key-value is applied, this request can be returned to the client.
	sig := make(chan struct{})
	s.monitorKV[string(uid)] = sig

	select {
	case <-sig:
		return nil
	case <-timeOutC.C:
		return errors.New("request time out")
	}
}

func (s *KvService) Lookup(key []byte) (*marshal.BytesKV, error) {
	kv, err := s.Storage.Get(key)
	if err != nil {
		return nil, err
	}
	return &marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value}, nil
}

func (s *KvService) Scan(lowKey, highKey []byte) ([]*marshal.BytesKV, error) {
	//todo
	return nil, nil
}
