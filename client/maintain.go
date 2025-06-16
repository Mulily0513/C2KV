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
	"github.com/Mulily0513/C2KV/c2kvserverpb"
	"google.golang.org/grpc"
)

type (
	StatusRequest  c2kvserverpb.StatusRequest
	StatusResponse c2kvserverpb.StatusResponse
)

type Maintain interface {
	// MemberList lists the current cluster membership.
	Status(ctx context.Context) (*StatusResponse, error)
}

type maintain struct {
	remote   c2kvserverpb.MaintenanceClient
	callOpts []grpc.CallOption
}

func NewMaintain(c *Client) Maintain {
	api := &maintain{remote: c2kvserverpb.NewMaintenanceClient(c.conn)}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func (m maintain) Status(ctx context.Context) (*StatusResponse, error) {
	resp, err := m.remote.Status(ctx, &c2kvserverpb.StatusRequest{}, m.callOpts...)
	if err == nil {
		return (*StatusResponse)(resp), nil
	}
	return nil, err
}
