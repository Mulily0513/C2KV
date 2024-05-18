// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"google.golang.org/grpc"
)

func NewClient(t *testing.T, cfg Config) (*Client, error) {
	cfg.Logger = zaptest.NewLogger(t)
	return New(cfg)
}

func TestDialCancel(t *testing.T) {
	testutil.RegisterLeakDetection(t)

	// accept first connection so client is created with dial timeout
	ln, err := net.Listen("unix", "dialcancel:12345")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ep := "unix://dialcancel:12345"
	cfg := Config{
		Endpoints:   []string{ep},
		DialTimeout: 30 * time.Second}
	c, err := NewClient(t, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// connect to ipv4 black hole so dial blocks
	c.SetEndpoints("http://254.0.0.1:12345")

	// issue Get to force redial attempts
	getc := make(chan struct{})
	go func() {
		defer close(getc)
		// Get may hang forever on grpc's Stream.Header() if its
		// context is never canceled.
		c.Get(c.Ctx(), "abc")
	}()

	// wait a little bit so client close is after dial starts
	time.Sleep(100 * time.Millisecond)

	donec := make(chan struct{})
	go func() {
		defer close(donec)
		c.Close()
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("failed to close")
	case <-donec:
	}
	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("get failed to exit")
	case <-getc:
	}
}

func TestDialTimeout(t *testing.T) {
	testutil.RegisterLeakDetection(t)

	wantError := context.DeadlineExceeded

	// grpc.WithBlock to block until connection up or timeout
	testCfgs := []Config{
		{
			Endpoints:   []string{"http://254.0.0.1:12345"},
			DialTimeout: 2 * time.Second,
			DialOptions: []grpc.DialOption{grpc.WithBlock()},
		},
		{
			Endpoints:   []string{"http://254.0.0.1:12345"},
			DialTimeout: time.Second,
			DialOptions: []grpc.DialOption{grpc.WithBlock()},
			Username:    "abc",
			Password:    "def",
		},
	}

	for i, cfg := range testCfgs {
		donec := make(chan error, 1)
		go func(cfg Config) {
			// without timeout, dial continues forever on ipv4 black hole
			c, err := NewClient(t, cfg)
			if c != nil || err == nil {
				t.Errorf("#%d: new client should fail", i)
			}
			donec <- err
		}(cfg)

		time.Sleep(10 * time.Millisecond)

		select {
		case err := <-donec:
			t.Errorf("#%d: dial didn't wait (%v)", i, err)
		default:
		}

		select {
		case <-time.After(5 * time.Second):
			t.Errorf("#%d: failed to timeout dial on time", i)
		case err := <-donec:
			if err.Error() != wantError.Error() {
				t.Errorf("#%d: unexpected error '%v', want '%v'", i, err, wantError)
			}
		}
	}
}

func TestDialNoTimeout(t *testing.T) {
	cfg := Config{Endpoints: []string{"127.0.0.1:12345"}}
	c, err := NewClient(t, cfg)
	if c == nil || err != nil {
		t.Fatalf("new client with DialNoWait should succeed, got %v", err)
	}
	c.Close()
}

func TestMaxUnaryRetries(t *testing.T) {
	maxUnaryRetries := uint(10)
	cfg := Config{
		Endpoints:       []string{"127.0.0.1:12345"},
		MaxUnaryRetries: maxUnaryRetries,
	}
	c, err := NewClient(t, cfg)
	if c == nil || err != nil {
		t.Fatalf("new client with MaxUnaryRetries should succeed, got %v", err)
	}
	defer c.Close()

	if c.cfg.MaxUnaryRetries != maxUnaryRetries {
		t.Fatalf("client MaxUnaryRetries should be %d, got %d", maxUnaryRetries, c.cfg.MaxUnaryRetries)
	}
}

func TestBackoff(t *testing.T) {
	backoffWaitBetween := 100 * time.Millisecond
	cfg := Config{
		Endpoints:          []string{"127.0.0.1:12345"},
		BackoffWaitBetween: backoffWaitBetween,
	}
	c, err := NewClient(t, cfg)
	if c == nil || err != nil {
		t.Fatalf("new client with BackoffWaitBetween should succeed, got %v", err)
	}
	defer c.Close()

	if c.cfg.BackoffWaitBetween != backoffWaitBetween {
		t.Fatalf("client BackoffWaitBetween should be %v, got %v", backoffWaitBetween, c.cfg.BackoffWaitBetween)
	}
}

func TestBackoffJitterFraction(t *testing.T) {
	backoffJitterFraction := float64(0.9)
	cfg := Config{
		Endpoints:             []string{"127.0.0.1:12345"},
		BackoffJitterFraction: backoffJitterFraction,
	}
	c, err := NewClient(t, cfg)
	if c == nil || err != nil {
		t.Fatalf("new client with BackoffJitterFraction should succeed, got %v", err)
	}
	defer c.Close()

	if c.cfg.BackoffJitterFraction != backoffJitterFraction {
		t.Fatalf("client BackoffJitterFraction should be %v, got %v", backoffJitterFraction, c.cfg.BackoffJitterFraction)
	}
}

func TestCloseCtxClient(t *testing.T) {
	ctx := context.Background()
	c := NewCtxClient(ctx)
	err := c.Close()
	// Close returns ctx.toErr, a nil error means an open Done channel
	if err == nil {
		t.Errorf("failed to Close the client. %v", err)
	}
}

func TestWithLogger(t *testing.T) {
	ctx := context.Background()
	c := NewCtxClient(ctx)
	if c.lg == nil {
		t.Errorf("unexpected nil in *zap.Logger")
	}

	c.WithLogger(nil)
	if c.lg != nil {
		t.Errorf("WithLogger should modify *zap.Logger")
	}
}

func TestZapWithLogger(t *testing.T) {
	ctx := context.Background()
	lg := zap.NewNop()
	c := NewCtxClient(ctx, WithZapLogger(lg))

	if c.lg != lg {
		t.Errorf("WithZapLogger should modify *zap.Logger")
	}
}

type mockAuthServer struct {
	*etcdserverpb.UnimplementedAuthServer
}

func (mockAuthServer) Authenticate(context.Context, *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	return &etcdserverpb.AuthenticateResponse{Token: "mock-token"}, nil
}

func TestSyncFiltersMembers(t *testing.T) {
	c, _ := NewClient(t, Config{Endpoints: []string{"http://254.0.0.1:12345"}})
	defer c.Close()
	c.Cluster = &mockCluster{
		[]*etcdserverpb.Member{
			{ID: 0, Name: "", ClientURLs: []string{"http://254.0.0.1:12345"}, IsLearner: false},
			{ID: 1, Name: "isStarted", ClientURLs: []string{"http://254.0.0.2:12345"}, IsLearner: true},
			{ID: 2, Name: "isStartedAndNotLearner", ClientURLs: []string{"http://254.0.0.3:12345"}, IsLearner: false},
		},
	}
	c.Sync(context.Background())

	endpoints := c.Endpoints()
	if len(endpoints) != 1 || endpoints[0] != "http://254.0.0.3:12345" {
		t.Error("Client.Sync uses learner and/or non-started member client URLs")
	}
}

type mockCluster struct {
	members []*etcdserverpb.Member
}

func (mc *mockCluster) MemberList(ctx context.Context) (*MemberListResponse, error) {
	return &MemberListResponse{Members: mc.members}, nil
}

func (mc *mockCluster) MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	return nil, nil
}

func (mc *mockCluster) MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	return nil, nil
}

func (mc *mockCluster) MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error) {
	return nil, nil
}

func (mc *mockCluster) MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberUpdateResponse, error) {
	return nil, nil
}

func (mc *mockCluster) MemberPromote(ctx context.Context, id uint64) (*MemberPromoteResponse, error) {
	return nil, nil
}

type mockMaintenance struct {
	Version map[string]string
}

func (mm mockMaintenance) Status(ctx context.Context, endpoint string) (*StatusResponse, error) {
	return &StatusResponse{Version: mm.Version[endpoint]}, nil
}

func (mm mockMaintenance) AlarmList(ctx context.Context) (*AlarmResponse, error) {
	return nil, nil
}

func (mm mockMaintenance) AlarmDisarm(ctx context.Context, m *AlarmMember) (*AlarmResponse, error) {
	return nil, nil
}

func (mm mockMaintenance) Defragment(ctx context.Context, endpoint string) (*DefragmentResponse, error) {
	return nil, nil
}

func (mm mockMaintenance) HashKV(ctx context.Context, endpoint string, rev int64) (*HashKVResponse, error) {
	return nil, nil
}

func (mm mockMaintenance) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (mm mockMaintenance) MoveLeader(ctx context.Context, transfereeID uint64) (*MoveLeaderResponse, error) {
	return nil, nil
}
