package app

import (
	"bytes"
	"context"
	"errors"
	"github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
	"github.com/Mulily0513/C2KV/internal/code"
	"github.com/Mulily0513/C2KV/internal/db"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/log"
	"github.com/Mulily0513/C2KV/internal/raft"
	"github.com/Mulily0513/C2KV/internal/telemetry"
	"google.golang.org/grpc"
	"net"
	"sort"
	"sync/atomic"
	"time"
)

type KvService struct {
	Storage    db.Storage
	proposeC   chan<- []byte
	monitor    *proposalMonitor
	ReqTimeout time.Duration
	RaftNode   raft.Node
	proposeSeq atomic.Uint64
}

var noPrefixEnd = []byte{0}
var maxRangeEnd = bytes.Repeat([]byte{0xff}, 1024)
var ErrRequestTimeout = errors.New("request time out")

func StartKVAPIService(proposeC chan<- []byte, requestTimeOut int, kvStorage db.Storage,
	monitor *proposalMonitor, localEAddr string, kvServiceStopC chan struct{}, raftNode raft.Node) {
	if monitor == nil {
		monitor = newProposalMonitor()
	}
	s := &KvService{
		Storage:    kvStorage,
		proposeC:   proposeC,
		monitor:    monitor,
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

func (s *KvService) Propose(ctx context.Context, key, val []byte, isDelete bool) error {
	if ctx == nil {
		ctx = context.Background()
	}

	id := s.proposeSeq.Add(1)
	// Raft index is the primary recency signal for merge/dedup; avoid per-request wall-clock syscall here.
	buf := marshal.EncodeProposalKV(id, key, val, isDelete, 0)

	// monitor this key-value. When this key-value is applied, this request can be returned to the client.
	sig := s.monitor.acquireSignal()
	defer s.monitor.releaseSignal(sig)
	s.monitor.put(id, sig)

	var (
		timer           *time.Timer
		timeoutC        <-chan time.Time
		enableSrvTimout = s.ReqTimeout > 0
	)
	if enableSrvTimout {
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= s.ReqTimeout {
			enableSrvTimout = false
		}
	}
	if enableSrvTimout {
		timer = time.NewTimer(s.ReqTimeout)
		timeoutC = timer.C
		defer timer.Stop()
	}

	enqueueStart := time.Now()
	select {
	case s.proposeC <- buf:
		telemetry.ObserveProposeWait(time.Since(enqueueStart))
	case <-ctx.Done():
		s.monitor.delete(id)
		return ctx.Err()
	case <-timeoutC:
		s.monitor.delete(id)
		return ErrRequestTimeout
	}

	select {
	case <-sig:
		return nil
	case <-ctx.Done():
		s.monitor.delete(id)
		return ctx.Err()
	case <-timeoutC:
		s.monitor.delete(id)
		return ErrRequestTimeout
	}
}

func (s *KvService) Lookup(key []byte) (*marshal.BytesKV, error) {
	kv, err := s.Storage.Get(key)
	if err != nil {
		return nil, err
	}
	if kv == nil || kv.Data == nil || kv.Data.Type == marshal.TypeDelete {
		return nil, code.ErrRecordNotExists
	}
	return &marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value}, nil
}

func (s *KvService) Scan(lowKey, highKey []byte) ([]*marshal.BytesKV, error) {
	fromKey := bytes.Equal(highKey, noPrefixEnd)
	scanHighKey := highKey
	if fromKey {
		scanHighKey = maxRangeEnd
	}
	kvs, err := s.Storage.Scan(lowKey, scanHighKey)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, nil
	}
	// Fast path for normal storage contract: sorted unique keys.
	result := make([]*marshal.BytesKV, 0, len(kvs))
	filtered := make([]*marshal.KV, 0, len(kvs))
	var prevKey []byte
	needNormalize := false
	for _, kv := range kvs {
		if kv == nil || kv.Data == nil {
			needNormalize = true
			break
		}
		if len(lowKey) > 0 && bytes.Compare(kv.Key, lowKey) < 0 {
			continue
		}
		if len(highKey) > 0 && !fromKey && bytes.Compare(kv.Key, highKey) >= 0 {
			continue
		}
		filtered = append(filtered, kv)
		if prevKey != nil && bytes.Compare(prevKey, kv.Key) >= 0 {
			needNormalize = true
		}
		prevKey = kv.Key
	}

	if !needNormalize {
		for _, kv := range filtered {
			if kv.Data.Type == marshal.TypeDelete {
				continue
			}
			result = append(result, &marshal.BytesKV{Key: kv.Key, Value: kv.Data.Value})
		}
		if len(result) == 0 {
			return nil, nil
		}
		return result, nil
	}

	// Fallback for non-normalized sources (tests/mocks): sort + latest dedup.
	sort.Slice(filtered, func(i, j int) bool {
		ki, kj := filtered[i], filtered[j]
		if cmp := bytes.Compare(ki.Key, kj.Key); cmp != 0 {
			return cmp < 0
		}
		if ki.Data.Index != kj.Data.Index {
			return ki.Data.Index > kj.Data.Index
		}
		return ki.Data.TimeStamp > kj.Data.TimeStamp
	})

	result = result[:0]
	for i := 0; i < len(filtered); {
		latest := filtered[i]
		if latest.Data.Type != marshal.TypeDelete {
			result = append(result, &marshal.BytesKV{Key: latest.Key, Value: latest.Data.Value})
		}
		i++
		for i < len(filtered) && bytes.Equal(filtered[i].Key, latest.Key) {
			i++
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (s *KvService) DeleteRange(ctx context.Context, key, rangeEnd []byte, prevKV bool) (int64, []*marshal.BytesKV, error) {
	targetKVs := make([]*marshal.BytesKV, 0)
	if len(rangeEnd) == 0 {
		kv, err := s.Lookup(key)
		if err != nil {
			if errors.Is(err, code.ErrRecordNotExists) {
				return 0, nil, nil
			}
			return 0, nil, err
		}
		targetKVs = append(targetKVs, kv)
	} else {
		kvs, err := s.Scan(key, rangeEnd)
		if err != nil {
			return 0, nil, err
		}
		targetKVs = append(targetKVs, kvs...)
	}

	var deleted int64
	prevKVs := make([]*marshal.BytesKV, 0, len(targetKVs))
	for _, kv := range targetKVs {
		if prevKV {
			prevKVs = append(prevKVs, &marshal.BytesKV{
				Key:   append([]byte(nil), kv.Key...),
				Value: append([]byte(nil), kv.Value...),
			})
		}
		if err := s.Propose(ctx, kv.Key, nil, true); err != nil {
			return deleted, prevKVs, err
		}
		deleted++
	}
	return deleted, prevKVs, nil
}
