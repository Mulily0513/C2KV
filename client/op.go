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

import pb "github.com/Mulily0513/C2KV/api/c2kvserverpb"

type opType int

const (
	// A default Op has opType 0, which is invalid.
	tRange opType = iota + 1
	tPut
	tDeleteRange
	tTxn
)

var noPrefixEnd = []byte{0}

// Op represents an Operation that kv can execute.
type Op struct {
	t   opType
	key []byte
	end []byte

	// for range
	limit        int64
	serializable bool
	keysOnly     bool
	countOnly    bool
	minModRev    int64
	maxModRev    int64
	minCreateRev int64
	maxCreateRev int64

	// for range, watch
	rev int64

	// for watch, put, delete
	prevKV bool

	// for watch
	// fragmentation should be disabled by default
	// if true, split watch events when total exceeds
	// "--max-request-bytes" flag value + 512-byte
	fragment bool

	// for put
	ignoreValue bool
	ignoreLease bool

	// progressNotify is for progress updates.
	progressNotify bool
	// createdNotify is for created event
	createdNotify bool
	// filters for watchers
	filterPut    bool
	filterDelete bool

	// for put
	val []byte

	isOptsWithFromKey bool
	isOptsWithPrefix  bool
}

func (op Op) toRangeRequest() *pb.RangeRequest {
	if op.t != tRange {
		panic("op.t != tRange")
	}
	r := &pb.RangeRequest{
		Key:               op.key,
		RangeEnd:          op.end,
		Limit:             op.limit,
		Revision:          op.rev,
		Serializable:      op.serializable,
		KeysOnly:          op.keysOnly,
		CountOnly:         op.countOnly,
		MinModRevision:    op.minModRev,
		MaxModRevision:    op.maxModRev,
		MinCreateRevision: op.minCreateRev,
		MaxCreateRevision: op.maxCreateRev,
	}
	return r
}

func (op Op) toRequestOp() *pb.RequestOp {
	switch op.t {
	case tRange:
		return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: op.toRangeRequest()}}
	case tPut:
		r := &pb.PutRequest{Key: op.key, Value: op.val}
		return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: r}}
	default:
		panic("Unknown Op")
	}
}

func NewOp() *Op {
	return &Op{key: []byte("")}
}

// OpGet returns "get" operation based on given key and operation options.
func OpGet(key string, opts ...OpOption) Op {
	// WithPrefix and WithFromKey are not supported together
	if IsOptsWithPrefix(opts) && IsOptsWithFromKey(opts) {
		panic("`WithPrefix` and `WithFromKey` cannot be set at the same time, choose one")
	}
	ret := Op{t: tRange, key: []byte(key)}
	ret.applyOpts(opts)
	return ret
}

// OpDelete returns "delete" operation based on given key and operation options.
func OpDelete(key string, opts ...OpOption) Op {
	// WithPrefix and WithFromKey are not supported together
	if IsOptsWithPrefix(opts) && IsOptsWithFromKey(opts) {
		panic("`WithPrefix` and `WithFromKey` cannot be set at the same time, choose one")
	}
	ret := Op{t: tDeleteRange, key: []byte(key)}
	ret.applyOpts(opts)
	switch {
	case ret.limit != 0:
		panic("unexpected limit in delete")
	case ret.rev != 0:
		panic("unexpected revision in delete")
	case ret.serializable:
		panic("unexpected serializable in delete")
	case ret.countOnly:
		panic("unexpected countOnly in delete")
	case ret.minModRev != 0, ret.maxModRev != 0:
		panic("unexpected mod revision filter in delete")
	case ret.minCreateRev != 0, ret.maxCreateRev != 0:
		panic("unexpected create revision filter in delete")
	case ret.filterDelete, ret.filterPut:
		panic("unexpected filter in delete")
	case ret.createdNotify:
		panic("unexpected createdNotify in delete")
	}
	return ret
}

// OpPut returns "put" operation based on given key-value and operation options.
func OpPut(key, val string, opts ...OpOption) Op {
	ret := Op{t: tPut, key: []byte(key), val: []byte(val)}
	ret.applyOpts(opts)
	switch {
	case ret.end != nil:
		panic("unexpected range in put")
	case ret.limit != 0:
		panic("unexpected limit in put")
	case ret.rev != 0:
		panic("unexpected revision in put")
	case ret.serializable:
		panic("unexpected serializable in put")
	case ret.countOnly:
		panic("unexpected countOnly in put")
	case ret.minModRev != 0, ret.maxModRev != 0:
		panic("unexpected mod revision filter in put")
	case ret.minCreateRev != 0, ret.maxCreateRev != 0:
		panic("unexpected create revision filter in put")
	case ret.filterDelete, ret.filterPut:
		panic("unexpected filter in put")
	case ret.createdNotify:
		panic("unexpected createdNotify in put")
	}
	return ret
}

func (op *Op) applyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpOption configures Operations like Get, Put, Delete.
type OpOption func(*Op)

// IsOptsWithPrefix returns true if WithPrefix option is called in the given opts.
func IsOptsWithPrefix(opts []OpOption) bool {
	ret := NewOp()
	for _, opt := range opts {
		opt(ret)
	}

	return ret.isOptsWithPrefix
}

// IsOptsWithFromKey returns true if WithFromKey option is called in the given opts.
func IsOptsWithFromKey(opts []OpOption) bool {
	ret := NewOp()
	for _, opt := range opts {
		opt(ret)
	}

	return ret.isOptsWithFromKey
}
