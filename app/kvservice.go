package main

import (
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/google/uuid"
	"time"
)

type KvService struct {
	storage     db.Storage
	proposeC    chan<- []byte
	confChangeC chan pb.ConfChange
	monitorKV   map[int64]chan struct{}
	ReqTimeout  time.Duration
}

func NewKVService(proposeC chan<- []byte, confChangeC chan pb.ConfChange, raftConfig *config.RaftConfig, kvStorage db.Storage, monitorKV map[int64]chan struct{}, localEAddr string, kvServiceStopC chan struct{}) *KvService {
	s := &KvService{
		storage:     kvStorage,
		proposeC:    proposeC,
		monitorKV:   monitorKV,
		ReqTimeout:  time.Duration(raftConfig.RequestTimeout) * time.Second,
		confChangeC: confChangeC,
	}
	ServeKVAPI(s, localEAddr, kvServiceStopC)
	return s
}

func (s *KvService) Propose(key, val []byte, delete bool) (bool, error) {
	timeOutC := time.NewTimer(s.ReqTimeout)
	uid := int64(uuid.New().ID())
	kv := new(marshal.KV)
	kv.Key = key
	kv.Data.Value = val
	kv.Data.TimeStamp = time.Now().Unix()
	kv.ApplySig = uid
	if delete {
		kv.Data.Type = marshal.TypeDelete
	}
	buf := marshal.EncodeKV(kv)
	s.proposeC <- buf

	//监听该kv，当该kv被applied时返回客户端
	sig := make(chan struct{})
	s.monitorKV[uid] = sig

	select {
	case <-sig:
		return true, nil
	case <-timeOutC.C:
		return false, errors.New("request time out")
	}
}

func (s *KvService) Lookup(key []byte) (*marshal.BytesKV, error) {
	kv, err := s.storage.Get(key)

	//handle error
	if kv.Data.Type == marshal.TypeDelete {
		return true, nil
	}

	return
}

func (s *KvService) Scan(lowKey, highKey []byte) ([]*marshal.BytesKV, error) {
	kvs, err := s.storage.Scan(lowKey, highKey)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
