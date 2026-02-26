package c2kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Mulily0513/C2KV/api/gen/c2kvserverpb"
)

const (
	c2kvEndpoints      = "c2kv.endpoints"
	c2kvDialTimeout    = "c2kv.dial_timeout"
	c2kvRequestTimeout = "c2kv.request_timeout"
	c2kvMaxRetry       = "c2kv.max_retry_attempts"
	c2kvRetryBackoffMs = "c2kv.retry_backoff_ms"
)

var fromKeyEnd = []byte{0}

type c2kvCreator struct{}

type c2kvDB struct {
	p              *properties.Properties
	endpoints      []string
	dialTimeout    time.Duration
	requestTimeout time.Duration
	maxRetry       int
	retryBackoff   time.Duration

	mu       sync.RWMutex
	leader   string
	conn     *grpc.ClientConn
	kv       c2kvserverpb.KVClient
	maintain c2kvserverpb.MaintenanceClient
}

func init() {
	ycsb.RegisterDBCreator("c2kv", c2kvCreator{})
}

func (c c2kvCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	epRaw := p.GetString(c2kvEndpoints, "127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343")
	endpoints := splitAndTrim(epRaw)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s is empty", c2kvEndpoints)
	}

	db := &c2kvDB{
		p:              p,
		endpoints:      endpoints,
		dialTimeout:    p.GetDuration(c2kvDialTimeout, 2*time.Second),
		requestTimeout: p.GetDuration(c2kvRequestTimeout, 5*time.Second),
		maxRetry:       p.GetInt(c2kvMaxRetry, 8),
		retryBackoff:   time.Duration(p.GetInt(c2kvRetryBackoffMs, 20)) * time.Millisecond,
	}
	if db.maxRetry < 1 {
		db.maxRetry = 1
	}
	if db.retryBackoff < 0 {
		db.retryBackoff = 0
	}
	if err := db.refreshLeader(context.Background()); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *c2kvDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.conn != nil {
		return db.conn.Close()
	}
	return nil
}

func (db *c2kvDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *c2kvDB) CleanupThread(_ context.Context) {}

func rowKey(table, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (db *c2kvDB) Read(ctx context.Context, table, key string, _ []string) (map[string][]byte, error) {
	k := rowKey(table, key)
	readRetry := db.maxRetry
	if readRetry > 3 {
		readRetry = 3
	}
	if readRetry < 1 {
		readRetry = 1
	}
	var lastErr error
	for i := 0; i < readRetry; i++ {
		resp, err := db.rangeWithRetry(ctx, &c2kvserverpb.RangeRequest{Key: []byte(k)})
		if err != nil {
			lastErr = err
			if i+1 < readRetry {
				if db.retryBackoff > 0 {
					time.Sleep(db.retryBackoff)
				}
				continue
			}
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			lastErr = fmt.Errorf("could not find value for key [%s]", k)
			if i+1 < readRetry {
				if db.retryBackoff > 0 {
					time.Sleep(db.retryBackoff)
				}
				continue
			}
			return nil, lastErr
		}
		var out map[string][]byte
		if err := json.NewDecoder(bytes.NewReader(resp.Kvs[0].Value)).Decode(&out); err != nil {
			return nil, err
		}
		return out, nil
	}
	return nil, lastErr
}

func (db *c2kvDB) Scan(ctx context.Context, table, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	resp, err := db.rangeWithRetry(ctx, &c2kvserverpb.RangeRequest{
		Key:      []byte(rowKey(table, startKey)),
		RangeEnd: fromKeyEnd,
		Limit:    int64(count),
	})
	if err != nil {
		return nil, err
	}
	out := make([]map[string][]byte, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var item map[string][]byte
		if err := json.NewDecoder(bytes.NewReader(kv.Value)).Decode(&item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func (db *c2kvDB) Update(ctx context.Context, table, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	_, err = db.putWithRetry(ctx, &c2kvserverpb.PutRequest{
		Key:   []byte(rowKey(table, key)),
		Value: data,
	})
	return err
}

func (db *c2kvDB) Insert(ctx context.Context, table, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *c2kvDB) Delete(ctx context.Context, table, key string) error {
	_, err := db.deleteWithRetry(ctx, &c2kvserverpb.DeleteRangeRequest{Key: []byte(rowKey(table, key))})
	return err
}

func (db *c2kvDB) rangeWithRetry(ctx context.Context, req *c2kvserverpb.RangeRequest) (*c2kvserverpb.RangeResponse, error) {
	var lastErr error
	for i := 0; i < db.maxRetry; i++ {
		reqCtx, cancel := db.withTimeout(ctx)
		kv, err := db.getKVClient()
		if err == nil {
			resp, rpcErr := kv.Range(reqCtx, req)
			cancel()
			if rpcErr == nil {
				return resp, nil
			}
			err = rpcErr
		} else {
			cancel()
		}
		lastErr = err
		if !shouldRetry(err) {
			return nil, err
		}
		if refreshErr := db.refreshLeader(ctx); refreshErr != nil {
			lastErr = err
		}
		if db.retryBackoff > 0 {
			time.Sleep(db.retryBackoff)
		}
	}
	return nil, lastErr
}

func (db *c2kvDB) putWithRetry(ctx context.Context, req *c2kvserverpb.PutRequest) (*c2kvserverpb.PutResponse, error) {
	var lastErr error
	for i := 0; i < db.maxRetry; i++ {
		reqCtx, cancel := db.withTimeout(ctx)
		kv, err := db.getKVClient()
		if err == nil {
			resp, rpcErr := kv.Put(reqCtx, req)
			cancel()
			if rpcErr == nil {
				return resp, nil
			}
			err = rpcErr
		} else {
			cancel()
		}
		lastErr = err
		if !shouldRetry(err) {
			return nil, err
		}
		if refreshErr := db.refreshLeader(ctx); refreshErr != nil {
			lastErr = err
		}
		if db.retryBackoff > 0 {
			time.Sleep(db.retryBackoff)
		}
	}
	return nil, lastErr
}

func (db *c2kvDB) deleteWithRetry(ctx context.Context, req *c2kvserverpb.DeleteRangeRequest) (*c2kvserverpb.DeleteRangeResponse, error) {
	var lastErr error
	for i := 0; i < db.maxRetry; i++ {
		reqCtx, cancel := db.withTimeout(ctx)
		kv, err := db.getKVClient()
		if err == nil {
			resp, rpcErr := kv.DeleteRange(reqCtx, req)
			cancel()
			if rpcErr == nil {
				return resp, nil
			}
			err = rpcErr
		} else {
			cancel()
		}
		lastErr = err
		if !shouldRetry(err) {
			return nil, err
		}
		if refreshErr := db.refreshLeader(ctx); refreshErr != nil {
			lastErr = err
		}
		if db.retryBackoff > 0 {
			time.Sleep(db.retryBackoff)
		}
	}
	return nil, lastErr
}

func (db *c2kvDB) getKVClient() (c2kvserverpb.KVClient, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.kv == nil {
		return nil, fmt.Errorf("c2kv client is not initialized")
	}
	return db.kv, nil
}

func (db *c2kvDB) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if db.requestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, db.requestTimeout)
}

func (db *c2kvDB) refreshLeader(ctx context.Context) error {
	var lastErr error
	for _, endpoint := range db.endpoints {
		conn, err := db.dial(ctx, endpoint)
		if err != nil {
			lastErr = err
			continue
		}
		maintain := c2kvserverpb.NewMaintenanceClient(conn)

		statusCtx, cancel := context.WithTimeout(ctx, db.dialTimeout)
		resp, err := maintain.Status(statusCtx, &c2kvserverpb.StatusRequest{})
		cancel()
		if err != nil {
			_ = conn.Close()
			lastErr = err
			continue
		}
		if !resp.IsLeader {
			_ = conn.Close()
			continue
		}

		db.mu.Lock()
		oldConn := db.conn
		db.conn = conn
		db.kv = c2kvserverpb.NewKVClient(conn)
		db.maintain = maintain
		db.leader = endpoint
		db.mu.Unlock()

		if oldConn != nil {
			_ = oldConn.Close()
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no leader found from endpoints: %v", db.endpoints)
	}
	return lastErr
}

func (db *c2kvDB) dial(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, db.dialTimeout)
	defer cancel()
	return grpc.DialContext(dialCtx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

func splitAndTrim(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.FailedPrecondition, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.Canceled, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}
