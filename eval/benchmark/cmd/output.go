package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/Mulily0513/C2KV/eval/report"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type benchSummary struct {
	TotalRequests   int            `json:"total_requests"`
	SuccessRequests int            `json:"success_requests"`
	FailedRequests  int            `json:"failed_requests"`
	ErrorRate       float64        `json:"error_rate"`
	TimeoutRate     float64        `json:"timeout_rate"`
	RequestsPerSec  float64        `json:"requests_per_sec"`
	TotalSeconds    float64        `json:"total_seconds"`
	FastestMs       float64        `json:"fastest_ms"`
	AverageMs       float64        `json:"average_ms"`
	P50Ms           float64        `json:"p50_ms"`
	P95Ms           float64        `json:"p95_ms"`
	P99Ms           float64        `json:"p99_ms"`
	SlowestMs       float64        `json:"slowest_ms"`
	ErrorDist       map[string]int `json:"error_dist,omitempty"`
}

type benchTimeSeriesPoint struct {
	UnixSecond   int64   `json:"unix_second"`
	MinLatencyMs float64 `json:"min_latency_ms"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	MaxLatencyMs float64 `json:"max_latency_ms"`
	Throughput   int64   `json:"throughput"`
}

type benchOutput struct {
	Name        string                     `json:"name"`
	ClusterName string                     `json:"cluster_name,omitempty"`
	EngineTag   string                     `json:"engine_tag,omitempty"`
	Timestamp   string                     `json:"timestamp"`
	Endpoints   []string                   `json:"endpoints"`
	Summary     benchSummary               `json:"summary"`
	PerType     map[string]benchSummary    `json:"per_type,omitempty"`
	EngineStats *benchEngineMetricsSummary `json:"engine_stats,omitempty"`
	TimeSeries  []benchTimeSeriesPoint     `json:"time_series,omitempty"`
}

func summarizeStats(s report.Stats) benchSummary {
	total := len(s.Lats)
	failed := 0
	timeout := 0
	for k, v := range s.ErrorDist {
		failed += v
		lk := strings.ToLower(k)
		if strings.Contains(lk, "deadline") || strings.Contains(lk, "timeout") || strings.Contains(lk, "time out") {
			timeout += v
		}
	}
	all := total + failed
	errRate := 0.0
	timeoutRate := 0.0
	if all > 0 {
		errRate = float64(failed) / float64(all)
		timeoutRate = float64(timeout) / float64(all)
	}
	avgMs := s.Average * 1000
	if total == 0 || math.IsNaN(avgMs) || math.IsInf(avgMs, 0) {
		avgMs = 0
	}
	return benchSummary{
		TotalRequests:   all,
		SuccessRequests: total,
		FailedRequests:  failed,
		ErrorRate:       errRate,
		TimeoutRate:     timeoutRate,
		RequestsPerSec:  s.RPS,
		TotalSeconds:    s.Total.Seconds(),
		FastestMs:       secondsAtPercentile(s.Lats, 0.0) * 1000,
		AverageMs:       avgMs,
		P50Ms:           secondsAtPercentile(s.Lats, 0.50) * 1000,
		P95Ms:           secondsAtPercentile(s.Lats, 0.95) * 1000,
		P99Ms:           secondsAtPercentile(s.Lats, 0.99) * 1000,
		SlowestMs:       secondsAtPercentile(s.Lats, 1.0) * 1000,
		ErrorDist:       s.ErrorDist,
	}
}

func secondsAtPercentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[n-1]
	}
	idx := int(float64(n-1) * p)
	return sorted[idx]
}

func toTimeSeriesPoints(ts report.TimeSeries) []benchTimeSeriesPoint {
	if len(ts) == 0 {
		return nil
	}
	out := make([]benchTimeSeriesPoint, 0, len(ts))
	for _, p := range ts {
		out = append(out, benchTimeSeriesPoint{
			UnixSecond:   p.Timestamp,
			MinLatencyMs: float64(p.MinLatency.Microseconds()) / 1000.0,
			AvgLatencyMs: float64(p.AvgLatency.Microseconds()) / 1000.0,
			MaxLatencyMs: float64(p.MaxLatency.Microseconds()) / 1000.0,
			Throughput:   p.ThroughPut,
		})
	}
	return out
}

func writeBenchmarkOutputs(name string, endpoints []string, overall report.Stats, perType map[string]report.Stats, engineStats *benchEngineMetricsSummary) error {
	output := benchOutput{
		Name:        name,
		ClusterName: clusterName,
		EngineTag:   engineTag,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Endpoints:   append([]string(nil), endpoints...),
		Summary:     summarizeStats(overall),
		EngineStats: engineStats,
		TimeSeries:  toTimeSeriesPoints(overall.TimeSeries),
	}
	if len(perType) > 0 {
		output.PerType = make(map[string]benchSummary, len(perType))
		for k, v := range perType {
			output.PerType[k] = summarizeStats(v)
		}
	}

	printSummary(output)

	if outputJSONPath != "" {
		if err := writeJSON(outputJSONPath, output); err != nil {
			return err
		}
	}
	if outputCSVPath != "" {
		if err := writeCSV(outputCSVPath, output.TimeSeries); err != nil {
			return err
		}
	}
	return nil
}

func printSummary(output benchOutput) {
	s := output.Summary
	fmt.Printf("\nSummary (%s):\n", output.Name)
	fmt.Printf("  requests_total: %d\n", s.TotalRequests)
	fmt.Printf("  requests_ok: %d\n", s.SuccessRequests)
	fmt.Printf("  requests_failed: %d\n", s.FailedRequests)
	fmt.Printf("  error_rate: %.4f\n", s.ErrorRate)
	fmt.Printf("  timeout_rate: %.4f\n", s.TimeoutRate)
	fmt.Printf("  rps: %.2f\n", s.RequestsPerSec)
	fmt.Printf("  latency_ms: p50=%.3f p95=%.3f p99=%.3f avg=%.3f\n", s.P50Ms, s.P95Ms, s.P99Ms, s.AverageMs)
	if len(output.PerType) > 0 {
		keys := make([]string, 0, len(output.PerType))
		for k := range output.PerType {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			ps := output.PerType[k]
			fmt.Printf("  [%s] rps=%.2f p99_ms=%.3f err_rate=%.4f\n", k, ps.RequestsPerSec, ps.P99Ms, ps.ErrorRate)
		}
	}
	if output.EngineStats != nil {
		es := output.EngineStats
		fmt.Printf("  engine_stats(nodes=%d/%d): propose(avg=%.3fms,count=%d) wal_write(avg=%.3fms,count=%d) wal_sync(avg=%.3fms,count=%d) apply(avg=%.3fms,count=%d)\n",
			es.NodesReported, es.NodesQueried,
			es.ProposeWait.AvgMs, es.ProposeWait.Count,
			es.WALWrite.AvgMs, es.WALWrite.Count,
			es.WALSync.AvgMs, es.WALSync.Count,
			es.Apply.AvgMs, es.Apply.Count,
		)
	}
}

func writeJSON(path string, data benchOutput) error {
	if err := ensureDir(path); err != nil {
		return err
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func writeCSV(path string, points []benchTimeSeriesPoint) error {
	if err := ensureDir(path); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err = w.Write([]string{"unix_second", "min_latency_ms", "avg_latency_ms", "max_latency_ms", "throughput"}); err != nil {
		return err
	}
	for _, p := range points {
		row := []string{
			fmt.Sprintf("%d", p.UnixSecond),
			fmt.Sprintf("%.3f", p.MinLatencyMs),
			fmt.Sprintf("%.3f", p.AvgLatencyMs),
			fmt.Sprintf("%.3f", p.MaxLatencyMs),
			fmt.Sprintf("%d", p.Throughput),
		}
		if err = w.Write(row); err != nil {
			return err
		}
	}
	return w.Error()
}

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0755)
}
