// Copyright 2021 The C2KV Authors
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

package resolver

import (
	"github.com/Mulily0513/C2KV/client/internal/endpoint"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/serviceconfig"
)

const (
	Schema = "c2kv-endpoints"
)

// C2KVManualResolver is a Resolver (and resolver.Builder) that can be updated
// using SetEndpoints.
type C2KVManualResolver struct {
	*manual.Resolver
	endpoints     []string
	serviceConfig *serviceconfig.ParseResult
}

func New(endpoints ...string) *C2KVManualResolver {
	r := manual.NewBuilderWithScheme(Schema)
	return &C2KVManualResolver{Resolver: r, endpoints: endpoints, serviceConfig: nil}
}

// Build returns itself for Resolver, because it's both a builder and a resolver.
func (r *C2KVManualResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.serviceConfig = cc.ParseServiceConfig(`{"loadBalancingPolicy": "round_robin"}`)
	if r.serviceConfig.Err != nil {
		return nil, r.serviceConfig.Err
	}
	res, err := r.Resolver.Build(target, cc, opts)
	if err != nil {
		return nil, err
	}
	// Populates endpoints stored in r into ClientConn (cc).
	r.updateState()
	return res, nil
}

func (r *C2KVManualResolver) SetEndpoints(endpoints []string) {
	r.endpoints = endpoints
	r.updateState()
}

func (r C2KVManualResolver) updateState() {
	if r.CC != nil {
		addresses := make([]resolver.Address, len(r.endpoints))
		for i, ep := range r.endpoints {
			addr, serverName := endpoint.Interpret(ep)
			addresses[i] = resolver.Address{Addr: addr, ServerName: serverName}
		}
		state := resolver.State{
			Addresses:     addresses,
			ServiceConfig: r.serviceConfig,
		}
		r.UpdateState(state)
	}
}
