package telemetry

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const statusNamePrefix = "c2kv-metrics:"

type EngineMetricsSnapshot struct {
	CapturedAtUnixMilli int64  `json:"captured_at_unix_milli"`
	Source              string `json:"source,omitempty"`

	ProposeWaitCount uint64 `json:"propose_wait_count"`
	ProposeWaitNanos uint64 `json:"propose_wait_nanos"`

	WALWriteCount uint64 `json:"wal_write_count"`
	WALWriteNanos uint64 `json:"wal_write_nanos"`

	WALSyncCount uint64 `json:"wal_sync_count"`
	WALSyncNanos uint64 `json:"wal_sync_nanos"`

	ApplyCount uint64 `json:"apply_count"`
	ApplyNanos uint64 `json:"apply_nanos"`
}

var (
	proposeWaitCount atomic.Uint64
	proposeWaitNanos atomic.Uint64
	walWriteCount    atomic.Uint64
	walWriteNanos    atomic.Uint64
	walSyncCount     atomic.Uint64
	walSyncNanos     atomic.Uint64
	applyCount       atomic.Uint64
	applyNanos       atomic.Uint64
)

func ObserveProposeWait(d time.Duration) {
	observeDuration(d, &proposeWaitCount, &proposeWaitNanos)
}

func ObserveWALWrite(d time.Duration) {
	observeDuration(d, &walWriteCount, &walWriteNanos)
}

func ObserveWALSync(d time.Duration) {
	observeDuration(d, &walSyncCount, &walSyncNanos)
}

func ObserveApply(d time.Duration) {
	observeDuration(d, &applyCount, &applyNanos)
}

func observeDuration(d time.Duration, counter *atomic.Uint64, nanos *atomic.Uint64) {
	if d < 0 {
		d = 0
	}
	counter.Add(1)
	nanos.Add(uint64(d.Nanoseconds()))
}

func Snapshot() EngineMetricsSnapshot {
	return EngineMetricsSnapshot{
		CapturedAtUnixMilli: time.Now().UnixMilli(),
		ProposeWaitCount:    proposeWaitCount.Load(),
		ProposeWaitNanos:    proposeWaitNanos.Load(),
		WALWriteCount:       walWriteCount.Load(),
		WALWriteNanos:       walWriteNanos.Load(),
		WALSyncCount:        walSyncCount.Load(),
		WALSyncNanos:        walSyncNanos.Load(),
		ApplyCount:          applyCount.Load(),
		ApplyNanos:          applyNanos.Load(),
	}
}

func (s EngineMetricsSnapshot) Add(other EngineMetricsSnapshot) EngineMetricsSnapshot {
	s.ProposeWaitCount += other.ProposeWaitCount
	s.ProposeWaitNanos += other.ProposeWaitNanos
	s.WALWriteCount += other.WALWriteCount
	s.WALWriteNanos += other.WALWriteNanos
	s.WALSyncCount += other.WALSyncCount
	s.WALSyncNanos += other.WALSyncNanos
	s.ApplyCount += other.ApplyCount
	s.ApplyNanos += other.ApplyNanos
	if other.CapturedAtUnixMilli > s.CapturedAtUnixMilli {
		s.CapturedAtUnixMilli = other.CapturedAtUnixMilli
	}
	return s
}

func (s EngineMetricsSnapshot) Sub(before EngineMetricsSnapshot) EngineMetricsSnapshot {
	s.ProposeWaitCount = subClamp(s.ProposeWaitCount, before.ProposeWaitCount)
	s.ProposeWaitNanos = subClamp(s.ProposeWaitNanos, before.ProposeWaitNanos)
	s.WALWriteCount = subClamp(s.WALWriteCount, before.WALWriteCount)
	s.WALWriteNanos = subClamp(s.WALWriteNanos, before.WALWriteNanos)
	s.WALSyncCount = subClamp(s.WALSyncCount, before.WALSyncCount)
	s.WALSyncNanos = subClamp(s.WALSyncNanos, before.WALSyncNanos)
	s.ApplyCount = subClamp(s.ApplyCount, before.ApplyCount)
	s.ApplyNanos = subClamp(s.ApplyNanos, before.ApplyNanos)
	return s
}

func subClamp(cur, prev uint64) uint64 {
	if cur < prev {
		return 0
	}
	return cur - prev
}

func EncodeStatusNameSnapshot(s EngineMetricsSnapshot) string {
	b, err := json.Marshal(s)
	if err != nil {
		return statusNamePrefix + "{}"
	}
	return statusNamePrefix + string(b)
}

func EncodeStatusName() string {
	return EncodeStatusNameSnapshot(Snapshot())
}

func DecodeStatusName(value string) (EngineMetricsSnapshot, bool, error) {
	if !strings.HasPrefix(value, statusNamePrefix) {
		return EngineMetricsSnapshot{}, false, nil
	}
	raw := strings.TrimPrefix(value, statusNamePrefix)
	var snapshot EngineMetricsSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return EngineMetricsSnapshot{}, true, fmt.Errorf("decode engine metrics snapshot failed: %w", err)
	}
	return snapshot, true, nil
}
