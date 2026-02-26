package cmd

import (
	"github.com/Mulily0513/C2KV/internal/telemetry"
	"testing"
)

func TestSummarizeEngineMetricsDelta(t *testing.T) {
	before := engineMetricsPollResult{
		snapshot: telemetry.EngineMetricsSnapshot{
			ProposeWaitCount: 10,
			ProposeWaitNanos: 10_000_000,
			WALWriteCount:    10,
			WALWriteNanos:    20_000_000,
			WALSyncCount:     5,
			WALSyncNanos:     15_000_000,
			ApplyCount:       10,
			ApplyNanos:       30_000_000,
		},
		nodesQueried:  3,
		nodesReported: 3,
	}
	after := engineMetricsPollResult{
		snapshot: telemetry.EngineMetricsSnapshot{
			ProposeWaitCount: 20,
			ProposeWaitNanos: 30_000_000,
			WALWriteCount:    25,
			WALWriteNanos:    50_000_000,
			WALSyncCount:     11,
			WALSyncNanos:     39_000_000,
			ApplyCount:       22,
			ApplyNanos:       66_000_000,
		},
		nodes:         []string{"127.0.0.1:2341"},
		nodesQueried:  3,
		nodesReported: 1,
	}

	out := summarizeEngineMetrics(before, after)
	if out == nil {
		t.Fatal("expected engine metrics summary")
	}
	if out.ProposeWait.Count != 10 {
		t.Fatalf("unexpected propose wait count: %d", out.ProposeWait.Count)
	}
	if out.ProposeWait.AvgMs != 2 {
		t.Fatalf("unexpected propose wait avg ms: %f", out.ProposeWait.AvgMs)
	}
	if out.WALWrite.Count != 15 {
		t.Fatalf("unexpected wal write count: %d", out.WALWrite.Count)
	}
	if out.WALWrite.AvgMs != 2 {
		t.Fatalf("unexpected wal write avg ms: %f", out.WALWrite.AvgMs)
	}
	if out.WALSync.AvgMs != 4 {
		t.Fatalf("unexpected wal sync avg ms: %f", out.WALSync.AvgMs)
	}
	if out.Apply.AvgMs != 3 {
		t.Fatalf("unexpected apply avg ms: %f", out.Apply.AvgMs)
	}
}
