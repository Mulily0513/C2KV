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
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Client provides and manages an c2kv client session.
type Client struct {
	KV
	Maintain

	conn *grpc.ClientConn //leader coon

	cfg Config

	callOpts []grpc.CallOption

	lg *zap.Logger
}

func FindLeaderClient(cfg Config) (*Client, string, error) {
	for i := 0; i < len(cfg.Endpoints); i++ {
		client, err := NewClient(cfg.Endpoints[i])
		if err != nil {
			return nil, "", errors.New("can not connect to c2kv cluster")
		}
		if statusRsp, err := client.Maintain.Status(context.TODO()); err == nil {
			if statusRsp.IsLeader {
				return client, cfg.Endpoints[i], nil
			}
		}
	}
	return nil, "", errors.New("no leader")
}

func NewClient(addr string) (*Client, error) {
	client := new(Client)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client.conn = conn
	client.Maintain = NewMaintain(client)
	client.KV = NewKV(client)
	client.lg, err = logutil.CreateDefaultZapLogger(C2KVClientDebugLevel())
	if client.lg != nil {
		client.lg = client.lg.Named("C2KV-client")
	}
	client.callOpts = defaultCallOpts
	return client, nil
}

// Close shuts down the client's etcd connections.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SetupOpts dialSetupOpts gives the dial opts prior to any authentication.
func (c *Client) SetupOpts(dopts ...grpc.DialOption) (opts []grpc.DialOption, err error) {
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
