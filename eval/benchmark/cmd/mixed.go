package cmd

import (
	"context"
	"fmt"
	"github.com/Mulily0513/C2KV/eval/report"
	"github.com/Mulily0513/C2KV/pkg/client"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
	mrand "math/rand"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var mixedCmd = &cobra.Command{
	Use:   "mixed",
	Short: "Benchmark mixed read/write workload",
	Run:   mixedFunc,
}

type mixedRequest struct {
	op   client.Op
	kind string
}

var (
	mixedKeySize      int
	mixedValSize      int
	mixedTotal        int
	mixedRate         int
	mixedReqTimeout   time.Duration
	mixedKeySpaceSize int
	mixedSeqKeys      bool
	mixedReadPercent  int
	mixedRangeSize    int
	mixedSerializable bool
	mixedWarmupWrites int
)

func init() {
	RootCmd.AddCommand(mixedCmd)
	mixedCmd.Flags().IntVar(&mixedKeySize, "key-size", 8, "Key size")
	mixedCmd.Flags().IntVar(&mixedValSize, "val-size", 8, "Value size for write path")
	mixedCmd.Flags().IntVar(&mixedTotal, "total", 10000, "Total operations")
	mixedCmd.Flags().IntVar(&mixedRate, "rate", 0, "Maximum operations per second (0 is no limit)")
	mixedCmd.Flags().DurationVar(&mixedReqTimeout, "req-timeout", 5*time.Second, "Per request timeout")
	mixedCmd.Flags().IntVar(&mixedKeySpaceSize, "key-space-size", 1000, "Maximum possible keys")
	mixedCmd.Flags().BoolVar(&mixedSeqKeys, "sequential-keys", false, "Use sequential keys")
	mixedCmd.Flags().IntVar(&mixedReadPercent, "read-percent", 50, "Read percentage in mixed workload")
	mixedCmd.Flags().IntVar(&mixedRangeSize, "range-size", 1, "Range window size for reads")
	mixedCmd.Flags().BoolVar(&mixedSerializable, "serializable", false, "Use serializable local reads")
	mixedCmd.Flags().IntVar(&mixedWarmupWrites, "warmup-writes", 1000, "Initial writes before mixed workload")
}

func mixedFunc(cmd *cobra.Command, args []string) {
	if mixedKeySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", mixedKeySpaceSize)
		os.Exit(1)
	}
	if mixedReadPercent < 0 || mixedReadPercent > 100 {
		fmt.Fprintf(os.Stderr, "expected --read-percent in [0,100], got (%v)", mixedReadPercent)
		os.Exit(1)
	}
	if mixedRangeSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --range-size, got (%v)", mixedRangeSize)
		os.Exit(1)
	}
	if mixedWarmupWrites < 0 {
		fmt.Fprintf(os.Stderr, "expected non-negative --warmup-writes, got (%v)", mixedWarmupWrites)
		os.Exit(1)
	}

	clients, _, err := mustCreateClients(totalClients, endpoints, targetLeader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create client error: %v", err)
		os.Exit(1)
	}
	defer closeClients(clients)
	engineBefore := collectEngineMetrics(endpoints, mixedReqTimeout)
	if warn := engineWarnings("engine metrics baseline", engineBefore); warn != "" {
		fmt.Fprintln(os.Stderr, warn)
	}

	if err = runWarmupWrites(clients); err != nil {
		fmt.Fprintf(os.Stderr, "warmup failed: %v\n", err)
		os.Exit(1)
	}

	requests := make(chan mixedRequest, totalClients)
	limit := rate.NewLimiter(rate.Limit(normalizeRate(mixedRate)), 1)
	value := string(mustRandBytes(mixedValSize))

	bar = pb.New(mixedTotal)
	bar.Format("Bom !")
	bar.Start()

	overallReport := newReport()
	readReport := newReport()
	writeReport := newReport()
	overallStatsC := overallReport.Stats()
	readStatsC := readReport.Stats()
	writeStatsC := writeReport.Stats()

	for i := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for req := range requests {
				_ = limit.Wait(context.Background())
				st := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), mixedReqTimeout)
				_, err := c.Do(ctx, req.op)
				cancel()
				res := report.Result{Err: err, Start: st, End: time.Now()}
				overallReport.Results() <- res
				if req.kind == "read" {
					readReport.Results() <- res
				} else {
					writeReport.Results() <- res
				}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		for i := 0; i < mixedTotal; i++ {
			idx := chooseKeyIndex(mixedSeqKeys, mixedKeySpaceSize, i)
			key := formatKey(idx, mixedKeySize)
			if mrand.Intn(100) < mixedReadPercent {
				opts := make([]client.OpOption, 0, 2)
				if mixedRangeSize > 1 {
					opts = append(opts, client.WithRange(formatKey(idx+mixedRangeSize, mixedKeySize)))
				}
				if mixedSerializable {
					opts = append(opts, client.WithSerializable())
				}
				requests <- mixedRequest{
					op:   client.OpGet(key, opts...),
					kind: "read",
				}
				continue
			}
			requests <- mixedRequest{
				op:   client.OpPut(key, value),
				kind: "write",
			}
		}
		close(requests)
	}()

	wg.Wait()
	close(overallReport.Results())
	close(readReport.Results())
	close(writeReport.Results())
	bar.Finish()

	overall := <-overallStatsC
	readStats := <-readStatsC
	writeStats := <-writeStatsC
	perType := map[string]report.Stats{
		"read":  readStats,
		"write": writeStats,
	}
	engineAfter := collectEngineMetrics(endpoints, mixedReqTimeout)
	if warn := engineWarnings("engine metrics final", engineAfter); warn != "" {
		fmt.Fprintln(os.Stderr, warn)
	}
	engineStats := summarizeEngineMetrics(engineBefore, engineAfter)
	if err = writeBenchmarkOutputs("mixed", endpoints, overall, perType, engineStats); err != nil {
		fmt.Fprintf(os.Stderr, "write benchmark output failed: %v\n", err)
		os.Exit(1)
	}
}

func runWarmupWrites(clients []*client.Client) error {
	if mixedWarmupWrites == 0 {
		return nil
	}
	value := string(mustRandBytes(mixedValSize))
	for i := 0; i < mixedWarmupWrites; i++ {
		idx := chooseKeyIndex(mixedSeqKeys, mixedKeySpaceSize, i)
		key := formatKey(idx, mixedKeySize)
		cli := clients[i%len(clients)]
		ctx, cancel := context.WithTimeout(context.Background(), mixedReqTimeout)
		_, err := cli.Do(ctx, client.OpPut(key, value))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
