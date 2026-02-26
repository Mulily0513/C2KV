package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	c2kvserverpb "github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
	"github.com/Mulily0513/C2KV/internal/code"
	"github.com/Mulily0513/C2KV/internal/db/marshal"
	"github.com/Mulily0513/C2KV/internal/db/mocks"
	"github.com/golang/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

func mockKV(key, val string, index uint64, typ int8) *marshal.KV {
	return &marshal.KV{
		Key: []byte(key),
		Data: &marshal.Data{
			Index: index,
			Type:  typ,
			Value: []byte(val),
		},
	}
}

func newTestKVService(storage *mocks.MockStorage, proposeBuffer int) (*KvService, chan []byte) {
	proposeC := make(chan []byte, proposeBuffer)
	return &KvService{
		Storage:    storage,
		proposeC:   proposeC,
		monitor:    newProposalMonitor(),
		ReqTimeout: 2 * time.Second,
		RaftNode:   newStaticStatusNode(true),
	}, proposeC
}

func ackDeleteProposals(t *testing.T, svc *KvService, proposeC <-chan []byte, expected int) []*marshal.KV {
	t.Helper()
	got := make([]*marshal.KV, 0, expected)
	for i := 0; i < expected; i++ {
		select {
		case raw := <-proposeC:
			kv := marshal.DecodeKV(raw)
			if len(kv.ApplySig) < 8 {
				t.Fatalf("invalid apply sig length: %d", len(kv.ApplySig))
			}
			id := binary.LittleEndian.Uint64(kv.ApplySig[:8])
			if id == 0 {
				t.Fatal("invalid apply sig id: 0")
			}
			sig, ok := svc.monitor.pop(id)
			if !ok || sig == nil {
				t.Fatalf("missing monitor channel for proposal id=%d", id)
			}
			select {
			case sig <- struct{}{}:
			default:
			}
			got = append(got, kv)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for proposal %d", i+1)
		}
	}
	return got
}

func assertKeys(t *testing.T, kvs []*c2kvserverpb.KeyValue, want ...string) {
	t.Helper()
	if len(kvs) != len(want) {
		t.Fatalf("unexpected kv count, want=%d got=%d", len(want), len(kvs))
	}
	for i := range kvs {
		if string(kvs[i].Key) != want[i] {
			t.Fatalf("unexpected key at %d, want=%q got=%q", i, want[i], string(kvs[i].Key))
		}
	}
}

func TestKVServiceRangePointFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("k1")).Return(mockKV("k1", "v1", 1, marshal.TypeInsert), nil)
	svc, _ := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Range returned error: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("expected 1 kv, got %d", len(resp.Kvs))
	}
	if string(resp.Kvs[0].Key) != "k1" || string(resp.Kvs[0].Value) != "v1" {
		t.Fatalf("unexpected kv: key=%q value=%q", string(resp.Kvs[0].Key), string(resp.Kvs[0].Value))
	}
}

func TestKVServiceRangePointNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("k1")).Return(nil, code.ErrRecordNotExists)
	svc, _ := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Range returned error: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("expected empty kvs, got %d", len(resp.Kvs))
	}
}

func TestKVServiceRangePointDeletedAsNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("k1")).Return(mockKV("k1", "", 3, marshal.TypeDelete), nil)
	svc, _ := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Range returned error: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("expected empty kvs for deleted key, got %d", len(resp.Kvs))
	}
}

func TestKVServiceRangeWithLimitAndKeysOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Scan([]byte("a"), []byte("z")).Return([]*marshal.KV{
		mockKV("b", "v2", 2, marshal.TypeInsert),
		mockKV("a", "v1", 1, marshal.TypeInsert),
		mockKV("c", "v3", 3, marshal.TypeInsert),
	}, nil)
	svc, _ := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{
		Key:      []byte("a"),
		RangeEnd: []byte("z"),
		Limit:    2,
		KeysOnly: true,
	})
	if err != nil {
		t.Fatalf("Range returned error: %v", err)
	}
	assertKeys(t, resp.Kvs, "a", "b")
	for _, kv := range resp.Kvs {
		if len(kv.Value) != 0 {
			t.Fatalf("keys_only should not return values, got value=%q", string(kv.Value))
		}
	}
}

func TestKVServiceRangeCountOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Scan([]byte("a"), []byte("z")).Return([]*marshal.KV{
		mockKV("a", "v1", 1, marshal.TypeInsert),
		mockKV("b", "v2", 2, marshal.TypeInsert),
	}, nil)
	svc, _ := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{
		Key:       []byte("a"),
		RangeEnd:  []byte("z"),
		CountOnly: true,
	})
	if err != nil {
		t.Fatalf("Range returned error: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("count_only should return empty kvs, got %d", len(resp.Kvs))
	}
}

func TestKVServiceRangeLinearizableRequiresLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	svc, _ := newTestKVService(storage, 1)
	svc.RaftNode = newStaticStatusNode(false)
	grpcSvc := &KVService{kvService: svc}

	_, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{Key: []byte("k1")})
	if err == nil {
		t.Fatal("expected error on follower linearizable read")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("unexpected code, want=%v got=%v", codes.FailedPrecondition, st.Code())
	}
}

func TestKVServiceRangeSerializableAllowsFollower(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("k1")).Return(mockKV("k1", "v1", 1, marshal.TypeInsert), nil)
	svc, _ := newTestKVService(storage, 1)
	svc.RaftNode = newStaticStatusNode(false)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.Range(context.Background(), &c2kvserverpb.RangeRequest{
		Key:          []byte("k1"),
		Serializable: true,
	})
	if err != nil {
		t.Fatalf("Range returned error: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Key) != "k1" {
		t.Fatalf("unexpected kv response: %+v", resp.Kvs)
	}
}

func TestKVServiceRangeRejectsUnsupportedFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	svc, _ := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	cases := []struct {
		name string
		req  *c2kvserverpb.RangeRequest
	}{
		{
			name: "revision",
			req: &c2kvserverpb.RangeRequest{
				Key:      []byte("k"),
				Revision: 1,
			},
		},
		{
			name: "sort order",
			req: &c2kvserverpb.RangeRequest{
				Key:       []byte("k"),
				SortOrder: c2kvserverpb.RangeRequest_ASCEND,
			},
		},
		{
			name: "revision filter",
			req: &c2kvserverpb.RangeRequest{
				Key:            []byte("k"),
				MinModRevision: 1,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := grpcSvc.Range(context.Background(), tc.req)
			if err == nil {
				t.Fatal("expected invalid argument error")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected grpc status error, got %v", err)
			}
			if st.Code() != codes.InvalidArgument {
				t.Fatalf("unexpected code, want=%v got=%v", codes.InvalidArgument, st.Code())
			}
		})
	}
}

func TestKvServiceScanDedupAndFilterDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Scan([]byte("a"), []byte("d")).Return([]*marshal.KV{
		mockKV("a", "v1", 1, marshal.TypeInsert),
		mockKV("a", "v2", 2, marshal.TypeInsert),
		mockKV("b", "deleted", 3, marshal.TypeDelete),
		mockKV("c", "v3", 4, marshal.TypeInsert),
	}, nil)
	svc, _ := newTestKVService(storage, 1)

	kvs, err := svc.Scan([]byte("a"), []byte("d"))
	if err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}
	if len(kvs) != 2 {
		t.Fatalf("expected 2 kvs after dedup+tombstone filter, got %d", len(kvs))
	}
	if !bytes.Equal(kvs[0].Key, []byte("a")) || !bytes.Equal(kvs[0].Value, []byte("v2")) {
		t.Fatalf("unexpected first kv: key=%q value=%q", string(kvs[0].Key), string(kvs[0].Value))
	}
	if !bytes.Equal(kvs[1].Key, []byte("c")) || !bytes.Equal(kvs[1].Value, []byte("v3")) {
		t.Fatalf("unexpected second kv: key=%q value=%q", string(kvs[1].Key), string(kvs[1].Value))
	}
}

func TestKvServiceScanFromKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Scan([]byte("k"), maxRangeEnd).Return([]*marshal.KV{
		mockKV("k", "v1", 1, marshal.TypeInsert),
		mockKV("z", "v2", 2, marshal.TypeInsert),
	}, nil)
	svc, _ := newTestKVService(storage, 1)

	kvs, err := svc.Scan([]byte("k"), noPrefixEnd)
	if err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}
	if len(kvs) != 2 {
		t.Fatalf("expected 2 kvs, got %d", len(kvs))
	}
}

func TestKVServiceDeleteRangeSingleKeyPrevKV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("k1")).Return(mockKV("k1", "v1", 1, marshal.TypeInsert), nil)
	svc, proposeC := newTestKVService(storage, 4)
	grpcSvc := &KVService{kvService: svc}

	done := make(chan []*marshal.KV, 1)
	go func() {
		done <- ackDeleteProposals(t, svc, proposeC, 1)
	}()

	resp, err := grpcSvc.DeleteRange(context.Background(), &c2kvserverpb.DeleteRangeRequest{
		Key:    []byte("k1"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatalf("DeleteRange returned error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Fatalf("expected deleted=1, got %d", resp.Deleted)
	}
	if len(resp.PrevKvs) != 1 || !bytes.Equal(resp.PrevKvs[0].Key, []byte("k1")) || !bytes.Equal(resp.PrevKvs[0].Value, []byte("v1")) {
		t.Fatalf("unexpected prev_kvs: %+v", resp.PrevKvs)
	}

	proposals := <-done
	if len(proposals) != 1 {
		t.Fatalf("expected 1 proposal, got %d", len(proposals))
	}
	if proposals[0].Data == nil || proposals[0].Data.Type != marshal.TypeDelete {
		t.Fatalf("expected delete proposal, got %+v", proposals[0].Data)
	}
	if !bytes.Equal(proposals[0].Key, []byte("k1")) {
		t.Fatalf("unexpected proposal key: %q", string(proposals[0].Key))
	}
}

func TestKVServiceDeleteRangeByScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Scan([]byte("a"), []byte("d")).Return([]*marshal.KV{
		mockKV("a", "v1", 1, marshal.TypeInsert),
		mockKV("b", "v2", 2, marshal.TypeInsert),
		mockKV("c", "v3", 3, marshal.TypeInsert),
	}, nil)
	svc, proposeC := newTestKVService(storage, 8)
	grpcSvc := &KVService{kvService: svc}

	done := make(chan []*marshal.KV, 1)
	go func() {
		done <- ackDeleteProposals(t, svc, proposeC, 3)
	}()

	resp, err := grpcSvc.DeleteRange(context.Background(), &c2kvserverpb.DeleteRangeRequest{
		Key:      []byte("a"),
		RangeEnd: []byte("d"),
		PrevKv:   false,
	})
	if err != nil {
		t.Fatalf("DeleteRange returned error: %v", err)
	}
	if resp.Deleted != 3 {
		t.Fatalf("expected deleted=3, got %d", resp.Deleted)
	}
	if len(resp.PrevKvs) != 0 {
		t.Fatalf("expected empty prev_kvs when PrevKv=false, got %d", len(resp.PrevKvs))
	}

	proposals := <-done
	if len(proposals) != 3 {
		t.Fatalf("expected 3 proposals, got %d", len(proposals))
	}
	for i, kv := range proposals {
		if kv.Data == nil || kv.Data.Type != marshal.TypeDelete {
			t.Fatalf("proposal %d is not delete: %+v", i, kv.Data)
		}
	}
}

func TestKVServiceDeleteRangeKeyNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("missing")).Return(nil, code.ErrRecordNotExists)
	svc, proposeC := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.DeleteRange(context.Background(), &c2kvserverpb.DeleteRangeRequest{
		Key:    []byte("missing"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatalf("DeleteRange returned error: %v", err)
	}
	if resp.Deleted != 0 {
		t.Fatalf("expected deleted=0, got %d", resp.Deleted)
	}
	if len(resp.PrevKvs) != 0 {
		t.Fatalf("expected empty prev_kvs, got %d", len(resp.PrevKvs))
	}

	select {
	case <-proposeC:
		t.Fatalf("unexpected proposal for missing key")
	default:
	}
}

func TestKVServiceDeleteRangeSingleKeyDeletedNoProposal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("deleted")).Return(mockKV("deleted", "", 5, marshal.TypeDelete), nil)
	svc, proposeC := newTestKVService(storage, 1)
	grpcSvc := &KVService{kvService: svc}

	resp, err := grpcSvc.DeleteRange(context.Background(), &c2kvserverpb.DeleteRangeRequest{
		Key:    []byte("deleted"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatalf("DeleteRange returned error: %v", err)
	}
	if resp.Deleted != 0 {
		t.Fatalf("expected deleted=0, got %d", resp.Deleted)
	}
	if len(resp.PrevKvs) != 0 {
		t.Fatalf("expected empty prev_kvs, got %d", len(resp.PrevKvs))
	}
	select {
	case <-proposeC:
		t.Fatal("unexpected proposal for deleted key")
	default:
	}
}

func TestKvServiceLookupNilKVAsNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Get([]byte("k1")).Return(nil, nil)
	svc, _ := newTestKVService(storage, 1)

	_, err := svc.Lookup([]byte("k1"))
	if !errors.Is(err, code.ErrRecordNotExists) {
		t.Fatalf("expected ErrRecordNotExists, got %v", err)
	}
}

func TestKVServiceDeleteRangeFromKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := mocks.NewMockStorage(ctrl)
	storage.EXPECT().Scan([]byte("b"), maxRangeEnd).Return([]*marshal.KV{
		mockKV("b", "v2", 2, marshal.TypeInsert),
		mockKV("c", "v3", 3, marshal.TypeInsert),
	}, nil)
	svc, proposeC := newTestKVService(storage, 8)
	grpcSvc := &KVService{kvService: svc}

	done := make(chan []*marshal.KV, 1)
	go func() {
		done <- ackDeleteProposals(t, svc, proposeC, 2)
	}()

	resp, err := grpcSvc.DeleteRange(context.Background(), &c2kvserverpb.DeleteRangeRequest{
		Key:      []byte("b"),
		RangeEnd: noPrefixEnd,
	})
	if err != nil {
		t.Fatalf("DeleteRange returned error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("expected deleted=2, got %d", resp.Deleted)
	}
	<-done
}
