package cmd

import (
	"github.com/Mulily0513/C2KV/eval/report"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSummarizeStats(t *testing.T) {
	stats := report.Stats{
		Average: 0.002,
		RPS:     1000,
		Total:   2 * time.Second,
		Lats:    []float64{0.001, 0.002, 0.003, 0.004},
		ErrorDist: map[string]int{
			"deadline exceeded": 1,
		},
	}

	s := summarizeStats(stats)
	if s.TotalRequests != 5 {
		t.Fatalf("unexpected total requests: %d", s.TotalRequests)
	}
	if s.FailedRequests != 1 {
		t.Fatalf("unexpected failed requests: %d", s.FailedRequests)
	}
	if s.TimeoutRate <= 0 {
		t.Fatalf("expected timeout rate > 0, got %f", s.TimeoutRate)
	}
	if s.P99Ms <= 0 {
		t.Fatalf("expected p99 > 0, got %f", s.P99Ms)
	}
}

func TestWriteCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench", "timeseries.csv")
	points := []benchTimeSeriesPoint{
		{UnixSecond: 1, MinLatencyMs: 1, AvgLatencyMs: 2, MaxLatencyMs: 3, Throughput: 10},
		{UnixSecond: 2, MinLatencyMs: 2, AvgLatencyMs: 3, MaxLatencyMs: 4, Throughput: 20},
	}
	if err := writeCSV(path, points); err != nil {
		t.Fatalf("writeCSV failed: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read csv failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("expected csv output")
	}
}
