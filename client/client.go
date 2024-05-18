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
	"errors"
	"fmt"
	"github.com/Mulily0513/C2KV/api/rpctypes"
	"github.com/Mulily0513/C2KV/client/internal/resolver"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

var (
	ErrNoAvailableEndpoints = errors.New("c2kvclient: no available endpoints")
)

// Client provides and manages an c2kv client session.
type Client struct {
	Cluster
	KV

	conn *grpc.ClientConn

	cfg Config
	//creds    grpccredentials.TransportCredentials
	resolver *resolver.C2KVManualResolver
	mu       *sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	callOpts []grpc.CallOption

	lgMu *sync.RWMutex
	lg   *zap.Logger
}

// New creates a new etcdv3 client from a given configuration.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, ErrNoAvailableEndpoints
	}
	return newClient(&cfg)
}

// NewCtxClient creates a client with a context but no underlying grpc
// connection. This is useful for embedded cases that override the
// service interface implementations and do not need connection management.
func NewCtxClient(ctx context.Context, opts ...Option) *Client {
	cctx, cancel := context.WithCancel(ctx)
	c := &Client{ctx: cctx, cancel: cancel, lgMu: new(sync.RWMutex), mu: new(sync.RWMutex)}
	for _, opt := range opts {
		opt(c)
	}
	if c.lg == nil {
		c.lg = zap.NewNop()
	}
	return c
}

// Option is a function type that can be passed as argument to NewCtxClient to configure client
type Option func(*Client)

// WithZapLogger is a NewCtxClient option that overrides the logger
func WithZapLogger(lg *zap.Logger) Option {
	return func(c *Client) {
		c.lg = lg
	}
}

// WithLogger overrides the logger.
//
// Deprecated: Please use WithZapLogger or Logger field in clientv3.Config
//
// Does not changes grpcLogger, that can be explicitly configured
// using grpc_zap.ReplaceGrpcLoggerV2(..) method.
func (c *Client) WithLogger(lg *zap.Logger) *Client {
	c.lgMu.Lock()
	c.lg = lg
	c.lgMu.Unlock()
	return c
}

// GetLogger gets the logger.
// NOTE: This method is for internal use of etcd-client library and should not be used as general-purpose logger.
func (c *Client) GetLogger() *zap.Logger {
	c.lgMu.RLock()
	l := c.lg
	c.lgMu.RUnlock()
	return l
}

// Close shuts down the client's etcd connections.
func (c *Client) Close() error {
	c.cancel()
	if c.conn != nil {
		return toErr(c.ctx, c.conn.Close())
	}
	return c.ctx.Err()
}

// Ctx is a context for "out of band" messages (e.g., for sending
// "clean up" message when another context is canceled). It is
// canceled on client Close().
func (c *Client) Ctx() context.Context { return c.ctx }

// Endpoints lists the registered endpoints for the client.
func (c *Client) Endpoints() []string {
	// copy the slice; protect original endpoints from being changed
	c.mu.RLock()
	defer c.mu.RUnlock()
	eps := make([]string, len(c.cfg.Endpoints))
	copy(eps, c.cfg.Endpoints)
	return eps
}

// SetEndpoints updates client's endpoints.
func (c *Client) SetEndpoints(eps ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Endpoints = eps

	c.resolver.SetEndpoints(eps)
}

// dialSetupOpts gives the dial opts prior to any authentication.
func (c *Client) dialSetupOpts(dopts ...grpc.DialOption) (opts []grpc.DialOption, err error) {
	if c.cfg.DialKeepAliveTime > 0 {
		params := keepalive.ClientParameters{
			Time:                c.cfg.DialKeepAliveTime,
			Timeout:             c.cfg.DialKeepAliveTimeout,
			PermitWithoutStream: c.cfg.PermitWithoutStream,
		}
		opts = append(opts, grpc.WithKeepaliveParams(params))
	}
	opts = append(opts, dopts...)
	opts = append(opts, grpc.WithInsecure())
	return opts, nil
}

// Dial connects to a single endpoint using the client's config.
func (c *Client) Dial(ep string) (*grpc.ClientConn, error) {
	// Using ad-hoc created resolver, to guarantee only explicitly given
	// endpoint is used.
	return c.dial(grpc.WithResolvers(resolver.New(ep)))
}

// dialWithBalancer dials the client's current load balanced resolver group.  The scheme of the host
// of the provided endpoint determines the scheme used for all endpoints of the client connection.
func (c *Client) dialWithBalancer(dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := append(dopts, grpc.WithResolvers(c.resolver))
	return c.dial(opts...)
}

// dial configures and dials any grpc balancer target.
func (c *Client) dial(dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts, err := c.dialSetupOpts(dopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to configure dialer: %v", err)
	}

	opts = append(opts, c.cfg.DialOptions...)

	dctx := c.ctx
	if c.cfg.DialTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(c.ctx, c.cfg.DialTimeout)
		defer cancel() // TODO: Is this right for cases where grpc.WithBlock() is not set on the dial options?
	}
	target := fmt.Sprintf("%s://%p/%s", resolver.Schema, c, authority(c.Endpoints()[0]))
	conn, err := grpc.DialContext(dctx, target, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func authority(endpoint string) string {
	spl := strings.SplitN(endpoint, "://", 2)
	if len(spl) < 2 {
		if strings.HasPrefix(endpoint, "unix:") {
			return endpoint[len("unix:"):]
		}
		if strings.HasPrefix(endpoint, "unixs:") {
			return endpoint[len("unixs:"):]
		}
		return endpoint
	}
	return spl[1]
}

func newClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = &Config{}
	}

	// use a temporary skeleton client to bootstrap first connection
	baseCtx := context.TODO()
	if cfg.Context != nil {
		baseCtx = cfg.Context
	}

	ctx, cancel := context.WithCancel(baseCtx)
	client := &Client{
		conn:     nil,
		cfg:      *cfg,
		ctx:      ctx,
		cancel:   cancel,
		mu:       new(sync.RWMutex),
		callOpts: defaultCallOpts,
		lgMu:     new(sync.RWMutex),
	}

	var err error
	if cfg.Logger != nil {
		client.lg = cfg.Logger
	} else if cfg.LogConfig != nil {
		client.lg, err = cfg.LogConfig.Build()
	} else {
		client.lg, err = logutil.CreateDefaultZapLogger(C2KVClientDebugLevel())
		if client.lg != nil {
			client.lg = client.lg.Named("C2KV-client")
		}
	}

	if err != nil {
		return nil, err
	}

	client.resolver = resolver.New(cfg.Endpoints...)
	if len(cfg.Endpoints) < 1 {
		client.cancel()
		return nil, fmt.Errorf("at least one Endpoint is required in client config")
	}

	// Use a provided endpoint target so that for https:// without any tls config given, then
	// grpc will assume the certificate server name is the endpoint host.
	conn, err := client.dialWithBalancer()
	if err != nil {
		client.cancel()
		client.resolver.Close()
		return nil, err
	}
	client.conn = conn

	client.Cluster = NewCluster(client)
	client.KV = NewKV(client)

	//go client.autoSync()
	return client, nil
}

func toErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	err = rpctypes.Error(err)
	if _, ok := err.(rpctypes.EtcdError); ok {
		return err
	}
	if ev, ok := status.FromError(err); ok {
		code := ev.Code()
		switch code {
		case codes.DeadlineExceeded:
			fallthrough
		case codes.Canceled:
			if ctx.Err() != nil {
				err = ctx.Err()
			}
		}
	}
	return err
}
