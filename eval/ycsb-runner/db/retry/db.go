package retry

import (
	"context"
	"time"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type DB struct {
	inner    ycsb.DB
	attempts int
	backoff  time.Duration
}

func Wrap(inner ycsb.DB, attempts int, backoff time.Duration) ycsb.DB {
	if attempts < 1 {
		attempts = 1
	}
	if backoff < 0 {
		backoff = 0
	}
	return &DB{
		inner:    inner,
		attempts: attempts,
		backoff:  backoff,
	}
}

func (d *DB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return d.inner.InitThread(ctx, threadID, threadCount)
}

func (d *DB) CleanupThread(ctx context.Context) { d.inner.CleanupThread(ctx) }
func (d *DB) Close() error                      { return d.inner.Close() }

func (d *DB) sleepBackoff(ctx context.Context) bool {
	if d.backoff <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d.backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (d *DB) retry(ctx context.Context, fn func() error) error {
	var err error
	for i := 0; i < d.attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if i+1 < d.attempts && !d.sleepBackoff(ctx) {
			return err
		}
	}
	return err
}

func (d *DB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var out map[string][]byte
	err := d.retry(ctx, func() error {
		var err error
		out, err = d.inner.Read(ctx, table, key, fields)
		return err
	})
	return out, err
}

func (d *DB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var out []map[string][]byte
	err := d.retry(ctx, func() error {
		var err error
		out, err = d.inner.Scan(ctx, table, startKey, count, fields)
		return err
	})
	return out, err
}

func (d *DB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return d.retry(ctx, func() error { return d.inner.Update(ctx, table, key, values) })
}

func (d *DB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return d.retry(ctx, func() error { return d.inner.Insert(ctx, table, key, values) })
}

func (d *DB) Delete(ctx context.Context, table string, key string) error {
	return d.retry(ctx, func() error { return d.inner.Delete(ctx, table, key) })
}
