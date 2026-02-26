package cmd

import (
	"context"
	"fmt"
	"github.com/Mulily0513/C2KV/internal/telemetry"
	"github.com/Mulily0513/C2KV/pkg/client"
	"sort"
	"strings"
	"time"
)

type engineMetricsPollResult struct {
	snapshot      telemetry.EngineMetricsSnapshot
	nodes         []string
	nodesQueried  int
	nodesReported int
	warnings      []string
}

type benchEngineStageSummary struct {
	Count   uint64  `json:"count"`
	TotalMs float64 `json:"total_ms"`
	AvgMs   float64 `json:"avg_ms"`
}

type benchEngineMetricsSummary struct {
	NodesQueried  int                     `json:"nodes_queried"`
	NodesReported int                     `json:"nodes_reported"`
	Nodes         []string                `json:"nodes,omitempty"`
	ProposeWait   benchEngineStageSummary `json:"propose_wait"`
	WALWrite      benchEngineStageSummary `json:"wal_write"`
	WALSync       benchEngineStageSummary `json:"wal_sync"`
	Apply         benchEngineStageSummary `json:"apply"`
}

func collectEngineMetrics(endpoints []string, timeout time.Duration) engineMetricsPollResult {
	unique := uniqueEndpoints(endpoints)
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	out := engineMetricsPollResult{
		nodes: make([]string, 0, len(unique)),
	}
	for _, endpoint := range unique {
		out.nodesQueried++
		cli, err := client.NewClient(endpoint)
		if err != nil {
			out.warnings = append(out.warnings, fmt.Sprintf("%s dial failed: %v", endpoint, err))
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := cli.Maintain.Status(ctx)
		cancel()
		_ = cli.Close()
		if err != nil {
			out.warnings = append(out.warnings, fmt.Sprintf("%s status failed: %v", endpoint, err))
			continue
		}

		snapshot, ok, err := telemetry.DecodeStatusName(resp.Name)
		if err != nil {
			out.warnings = append(out.warnings, fmt.Sprintf("%s decode metrics failed: %v", endpoint, err))
			continue
		}
		if !ok {
			continue
		}
		out.nodesReported++
		out.nodes = append(out.nodes, endpoint)
		out.snapshot = out.snapshot.Add(snapshot)
	}
	sort.Strings(out.nodes)
	return out
}

func summarizeEngineMetrics(before, after engineMetricsPollResult) *benchEngineMetricsSummary {
	if before.nodesReported == 0 && after.nodesReported == 0 {
		return nil
	}
	delta := after.snapshot.Sub(before.snapshot)
	return &benchEngineMetricsSummary{
		NodesQueried:  maxInt(before.nodesQueried, after.nodesQueried),
		NodesReported: after.nodesReported,
		Nodes:         append([]string(nil), after.nodes...),
		ProposeWait:   summarizeEngineStage(delta.ProposeWaitCount, delta.ProposeWaitNanos),
		WALWrite:      summarizeEngineStage(delta.WALWriteCount, delta.WALWriteNanos),
		WALSync:       summarizeEngineStage(delta.WALSyncCount, delta.WALSyncNanos),
		Apply:         summarizeEngineStage(delta.ApplyCount, delta.ApplyNanos),
	}
}

func summarizeEngineStage(count uint64, nanos uint64) benchEngineStageSummary {
	stage := benchEngineStageSummary{
		Count:   count,
		TotalMs: float64(nanos) / 1e6,
	}
	if count > 0 {
		stage.AvgMs = stage.TotalMs / float64(count)
	}
	return stage
}

func engineWarnings(prefix string, poll engineMetricsPollResult) string {
	if len(poll.warnings) == 0 {
		return ""
	}
	if len(poll.warnings) == 1 {
		return fmt.Sprintf("%s: %s", prefix, poll.warnings[0])
	}
	return fmt.Sprintf("%s: %s", prefix, strings.Join(poll.warnings, "; "))
}

func uniqueEndpoints(endpoints []string) []string {
	seen := make(map[string]struct{}, len(endpoints))
	out := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == "" {
			continue
		}
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		out = append(out, endpoint)
	}
	return out
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
