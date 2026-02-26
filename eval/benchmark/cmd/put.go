// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"github.com/Mulily0513/C2KV/eval/report"
	"github.com/Mulily0513/C2KV/pkg/client"
	"gopkg.in/cheggaaa/pb.v1"
	"os"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchmark put",

	Run: putFunc,
}

var (
	keySize int
	valSize int

	putTotal   int
	putRate    int
	reqTimeout time.Duration

	keySpaceSize int
	seqKeys      bool

	//compactInterval   time.Duration
	//compactIndexDelta int64

	checkHashkv bool
)

func init() {
	RootCmd.AddCommand(putCmd)
	putCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of put request")
	putCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of put request")
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")
	putCmd.Flags().DurationVar(&reqTimeout, "req-timeout", 5*time.Second, "Per request timeout")

	putCmd.Flags().IntVar(&putTotal, "total", 10000, "Total number of put requests")
	putCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	putCmd.Flags().BoolVar(&seqKeys, "sequential-keys", false, "Use sequential keys")
	//putCmd.Flags().DurationVar(&compactInterval, "compact-interval", 0, `Interval to compact database (do not duplicate this with etcd's 'auto-compaction-retention' flag) (e.g. --compact-interval=5m compacts every 5-minute)`)
	//putCmd.Flags().Int64Var(&compactIndexDelta, "compact-index-delta", 1000, "Delta between current revision and compact revision (e.g. current revision 10000, compact at 9000)")
	putCmd.Flags().BoolVar(&checkHashkv, "check-hashkv", false, "'true' to check hashkv")
}

func putFunc(cmd *cobra.Command, args []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", keySpaceSize)
		os.Exit(1)
	}

	requests := make(chan client.Op, totalClients)
	limit := rate.NewLimiter(rate.Limit(normalizeRate(putRate)), 1)
	clients, _, err := mustCreateClients(totalClients, endpoints, targetLeader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create client error: %v", err)
		os.Exit(1)
	}
	defer closeClients(clients)
	v := string(mustRandBytes(valSize))
	engineBefore := collectEngineMetrics(endpoints, reqTimeout)
	if warn := engineWarnings("engine metrics baseline", engineBefore); warn != "" {
		fmt.Fprintln(os.Stderr, warn)
	}

	bar = pb.New(putTotal)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()
	statsC := r.Stats()
	for i := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for op := range requests {
				limit.Wait(context.Background())

				st := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)
				_, err := c.Do(ctx, op)
				cancel()
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		for i := 0; i < putTotal; i++ {
			idx := chooseKeyIndex(seqKeys, keySpaceSize, i)
			requests <- client.OpPut(formatKey(idx, keySize), v)
		}
		close(requests)
	}()

	wg.Wait()
	close(r.Results())
	bar.Finish()
	overall := <-statsC
	engineAfter := collectEngineMetrics(endpoints, reqTimeout)
	if warn := engineWarnings("engine metrics final", engineAfter); warn != "" {
		fmt.Fprintln(os.Stderr, warn)
	}
	engineStats := summarizeEngineMetrics(engineBefore, engineAfter)
	if err = writeBenchmarkOutputs("put", endpoints, overall, nil, engineStats); err != nil {
		fmt.Fprintf(os.Stderr, "write benchmark output failed: %v\n", err)
		os.Exit(1)
	}
}
