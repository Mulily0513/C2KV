package cmd

import (
	"context"
	"fmt"
	"github.com/Mulily0513/C2KV/eval/report"
	"github.com/Mulily0513/C2KV/pkg/client"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var rangeCmd = &cobra.Command{
	Use:   "range",
	Short: "Benchmark range/get",
	Run:   rangeFunc,
}

var (
	rangeKeySize      int
	rangeTotal        int
	rangeRate         int
	rangeReqTimeout   time.Duration
	rangeKeySpaceSize int
	rangeSeqKeys      bool
	rangeSize         int
	rangeSerializable bool
	rangeKeysOnly     bool
	rangeCountOnly    bool
)

func init() {
	RootCmd.AddCommand(rangeCmd)
	rangeCmd.Flags().IntVar(&rangeKeySize, "key-size", 8, "Key size of range request")
	rangeCmd.Flags().IntVar(&rangeRate, "rate", 0, "Maximum ranges per second (0 is no limit)")
	rangeCmd.Flags().DurationVar(&rangeReqTimeout, "req-timeout", 5*time.Second, "Per request timeout")
	rangeCmd.Flags().IntVar(&rangeTotal, "total", 10000, "Total number of range requests")
	rangeCmd.Flags().IntVar(&rangeKeySpaceSize, "key-space-size", 1, "Maximum possible keys")
	rangeCmd.Flags().BoolVar(&rangeSeqKeys, "sequential-keys", false, "Use sequential keys")
	rangeCmd.Flags().IntVar(&rangeSize, "range-size", 1, "Range window size in keys (1 means point read)")
	rangeCmd.Flags().BoolVar(&rangeSerializable, "serializable", false, "Use serializable local reads")
	rangeCmd.Flags().BoolVar(&rangeKeysOnly, "keys-only", false, "Return keys only")
	rangeCmd.Flags().BoolVar(&rangeCountOnly, "count-only", false, "Return count only")
}

func rangeFunc(cmd *cobra.Command, args []string) {
	if rangeKeySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", rangeKeySpaceSize)
		os.Exit(1)
	}
	if rangeSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --range-size, got (%v)", rangeSize)
		os.Exit(1)
	}

	requests := make(chan client.Op, totalClients)
	limit := rate.NewLimiter(rate.Limit(normalizeRate(rangeRate)), 1)
	clients, _, err := mustCreateClients(totalClients, endpoints, targetLeader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create client error: %v", err)
		os.Exit(1)
	}
	defer closeClients(clients)
	engineBefore := collectEngineMetrics(endpoints, rangeReqTimeout)
	if warn := engineWarnings("engine metrics baseline", engineBefore); warn != "" {
		fmt.Fprintln(os.Stderr, warn)
	}

	bar = pb.New(rangeTotal)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()
	statsC := r.Stats()
	for i := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for op := range requests {
				_ = limit.Wait(context.Background())

				st := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), rangeReqTimeout)
				_, err := c.Do(ctx, op)
				cancel()
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		for i := 0; i < rangeTotal; i++ {
			idx := chooseKeyIndex(rangeSeqKeys, rangeKeySpaceSize, i)
			key := formatKey(idx, rangeKeySize)
			opts := make([]client.OpOption, 0, 4)
			if rangeSize > 1 {
				opts = append(opts, client.WithRange(formatKey(idx+rangeSize, rangeKeySize)))
			}
			if rangeSerializable {
				opts = append(opts, client.WithSerializable())
			}
			if rangeKeysOnly {
				opts = append(opts, client.WithKeysOnly())
			}
			if rangeCountOnly {
				opts = append(opts, client.WithCountOnly())
			}
			requests <- client.OpGet(key, opts...)
		}
		close(requests)
	}()

	wg.Wait()
	close(r.Results())
	bar.Finish()
	overall := <-statsC
	engineAfter := collectEngineMetrics(endpoints, rangeReqTimeout)
	if warn := engineWarnings("engine metrics final", engineAfter); warn != "" {
		fmt.Fprintln(os.Stderr, warn)
	}
	engineStats := summarizeEngineMetrics(engineBefore, engineAfter)
	if err = writeBenchmarkOutputs("range", endpoints, overall, nil, engineStats); err != nil {
		fmt.Fprintf(os.Stderr, "write benchmark output failed: %v\n", err)
		os.Exit(1)
	}
}
