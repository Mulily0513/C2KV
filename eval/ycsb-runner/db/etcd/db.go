package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/magiconair/properties"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	etcdEndpoints      = "etcd.endpoints"
	etcdDialTimeout    = "etcd.dial_timeout"
	etcdRequestTimeout = "etcd.request_timeout"
	etcdCertFile       = "etcd.cert_file"
	etcdKeyFile        = "etcd.key_file"
	etcdCaFile         = "etcd.cacert_file"
	etcdMaxRetry       = "etcd.max_retry_attempts"
	etcdRetryBackoffMs = "etcd.retry_backoff_ms"
)

type etcdCreator struct{}

type etcdDB struct {
	p              *properties.Properties
	client         *clientv3.Client
	requestTimeout time.Duration
	maxRetry       int
	retryBackoff   time.Duration
}

func init() {
	ycsb.RegisterDBCreator("etcd", etcdCreator{})
}

func (c etcdCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cfg, err := getClientConfig(p)
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(*cfg)
	if err != nil {
		return nil, err
	}
	db := &etcdDB{
		p:              p,
		client:         client,
		requestTimeout: p.GetDuration(etcdRequestTimeout, 5*time.Second),
		maxRetry:       p.GetInt(etcdMaxRetry, 8),
		retryBackoff:   time.Duration(p.GetInt(etcdRetryBackoffMs, 20)) * time.Millisecond,
	}
	if db.maxRetry < 1 {
		db.maxRetry = 1
	}
	if db.retryBackoff < 0 {
		db.retryBackoff = 0
	}
	return db, nil
}

func getClientConfig(p *properties.Properties) (*clientv3.Config, error) {
	endpoints := p.GetString(etcdEndpoints, "localhost:2379")
	dialTimeout := p.GetDuration(etcdDialTimeout, 2*time.Second)

	var tlsConfig *tls.Config
	if strings.Contains(endpoints, "https") {
		tlsInfo := transport.TLSInfo{
			CertFile:      p.MustGetString(etcdCertFile),
			KeyFile:       p.MustGetString(etcdKeyFile),
			TrustedCAFile: p.MustGetString(etcdCaFile),
		}
		c, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
		tlsConfig = c
	}

	return &clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
	}, nil
}

func (db *etcdDB) Close() error                                                 { return db.client.Close() }
func (db *etcdDB) InitThread(ctx context.Context, _ int, _ int) context.Context { return ctx }
func (db *etcdDB) CleanupThread(_ context.Context)                              {}

func getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (db *etcdDB) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if db.requestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, db.requestTimeout)
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unknown, codes.Canceled:
		return true
	default:
		return false
	}
}

func (db *etcdDB) retryBackoffSleep() {
	if db.retryBackoff > 0 {
		time.Sleep(db.retryBackoff)
	}
}

func (db *etcdDB) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	rkey := getRowKey(table, key)
	var lastErr error
	for i := 0; i < db.maxRetry; i++ {
		reqCtx, cancel := db.withTimeout(ctx)
		value, err := db.client.Get(reqCtx, rkey)
		cancel()
		if err != nil {
			lastErr = err
			if !shouldRetry(err) || i+1 >= db.maxRetry {
				return nil, err
			}
			db.retryBackoffSleep()
			continue
		}
		if value.Count == 0 {
			lastErr = fmt.Errorf("could not find value for key [%s]", rkey)
			if i+1 >= db.maxRetry {
				return nil, lastErr
			}
			db.retryBackoffSleep()
			continue
		}

		var r map[string][]byte
		if decodeErr := json.NewDecoder(bytes.NewReader(value.Kvs[0].Value)).Decode(&r); decodeErr != nil {
			return nil, decodeErr
		}
		return r, nil
	}
	return nil, lastErr
}

func (db *etcdDB) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	rkey := getRowKey(table, startKey)
	reqCtx, cancel := db.withTimeout(ctx)
	values, err := db.client.Get(reqCtx, rkey, clientv3.WithFromKey(), clientv3.WithLimit(int64(count)))
	cancel()
	if err != nil {
		return nil, err
	}
	if values.Count != int64(count) {
		return nil, fmt.Errorf("unexpected number of result for key [%s], expected %d but was %d", rkey, count, values.Count)
	}
	for _, v := range values.Kvs {
		var r map[string][]byte
		if err = json.NewDecoder(bytes.NewReader(v.Value)).Decode(&r); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func (db *etcdDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rkey := getRowKey(table, key)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	var lastErr error
	for i := 0; i < db.maxRetry; i++ {
		reqCtx, cancel := db.withTimeout(ctx)
		_, err = db.client.Put(reqCtx, rkey, string(data))
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if !shouldRetry(err) || i+1 >= db.maxRetry {
			return err
		}
		db.retryBackoffSleep()
	}
	return lastErr
}

func (db *etcdDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *etcdDB) Delete(ctx context.Context, table string, key string) error {
	var lastErr error
	for i := 0; i < db.maxRetry; i++ {
		reqCtx, cancel := db.withTimeout(ctx)
		_, err := db.client.Delete(reqCtx, getRowKey(table, key))
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if !shouldRetry(err) || i+1 >= db.maxRetry {
			return err
		}
		db.retryBackoffSleep()
	}
	return lastErr
}
