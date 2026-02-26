package telemetry

import (
	"strings"
	"testing"
	"time"
)

func TestSnapshotObserveAndSub(t *testing.T) {
	before := Snapshot()

	ObserveProposeWait(2 * time.Millisecond)
	ObserveWALWrite(3 * time.Millisecond)
	ObserveWALSync(4 * time.Millisecond)
	ObserveApply(5 * time.Millisecond)

	after := Snapshot()
	delta := after.Sub(before)

	if delta.ProposeWaitCount < 1 || delta.ProposeWaitNanos == 0 {
		t.Fatalf("expected propose wait metrics to increase, delta=%+v", delta)
	}
	if delta.WALWriteCount < 1 || delta.WALWriteNanos == 0 {
		t.Fatalf("expected wal write metrics to increase, delta=%+v", delta)
	}
	if delta.WALSyncCount < 1 || delta.WALSyncNanos == 0 {
		t.Fatalf("expected wal sync metrics to increase, delta=%+v", delta)
	}
	if delta.ApplyCount < 1 || delta.ApplyNanos == 0 {
		t.Fatalf("expected apply metrics to increase, delta=%+v", delta)
	}
}

func TestStatusNameEncodeDecode(t *testing.T) {
	snap := EngineMetricsSnapshot{
		CapturedAtUnixMilli: 123,
		ProposeWaitCount:    1,
		ProposeWaitNanos:    2,
		WALWriteCount:       3,
		WALWriteNanos:       4,
		WALSyncCount:        5,
		WALSyncNanos:        6,
		ApplyCount:          7,
		ApplyNanos:          8,
	}
	encoded := EncodeStatusNameSnapshot(snap)
	if !strings.HasPrefix(encoded, statusNamePrefix) {
		t.Fatalf("unexpected encoded prefix: %s", encoded)
	}

	got, ok, err := DecodeStatusName(encoded)
	if err != nil {
		t.Fatalf("DecodeStatusName failed: %v", err)
	}
	if !ok {
		t.Fatal("expected encoded status to be recognized")
	}
	if got.ProposeWaitCount != snap.ProposeWaitCount || got.ApplyNanos != snap.ApplyNanos {
		t.Fatalf("decoded snapshot mismatch: got=%+v want=%+v", got, snap)
	}
}
