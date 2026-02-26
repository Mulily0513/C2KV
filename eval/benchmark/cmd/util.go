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
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/eval/report"
	"github.com/Mulily0513/C2KV/pkg/client"
	"math"
	mrand "math/rand"
	"os"
)

func mustCreateClients(totalClient int, endpoints []string, toLeader bool) ([]*client.Client, string, error) {
	if totalClient <= 0 {
		return nil, "", fmt.Errorf("invalid clients: %d", totalClient)
	}
	if len(endpoints) == 0 {
		return nil, "", fmt.Errorf("empty endpoints")
	}

	conns := make([]*client.Client, 0, totalClient)
	var targetEndpoint string
	cfg := client.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	}

	if toLeader {
		leaderClient, leaderEndpoint, err := client.FindLeaderClient(cfg)
		if err != nil {
			return nil, "", err
		}
		targetEndpoint = leaderEndpoint
		conns = append(conns, leaderClient)

		for i := 1; i < totalClient; i++ {
			cli, err := client.NewClient(targetEndpoint)
			if err != nil {
				closeClients(conns)
				return nil, "", err
			}
			conns = append(conns, cli)
		}
		return conns, targetEndpoint, nil
	}

	for i := 0; i < totalClient; i++ {
		targetEndpoint = endpoints[i%len(endpoints)]
		cli, err := client.NewClient(targetEndpoint)
		if err != nil {
			closeClients(conns)
			return nil, "", err
		}
		conns = append(conns, cli)
	}
	return conns, targetEndpoint, nil
}

func closeClients(clients []*client.Client) {
	for _, c := range clients {
		if c != nil {
			_ = c.Close()
		}
	}
}

func mustRandBytes(n int) []byte {
	rb := make([]byte, n)
	_, err := rand.Read(rb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate value: %v\n", err)
		os.Exit(1)
	}
	return rb
}

func newReport() report.Report {
	p := "%4.4f"
	if precise {
		p = "%g"
	}
	if sample || outputJSONPath != "" || outputCSVPath != "" {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}

func normalizeRate(rate int) int {
	if rate == 0 {
		return math.MaxInt32
	}
	return rate
}

func chooseKeyIndex(seq bool, keySpace int, i int) int {
	if seq {
		return i % keySpace
	}
	return mrand.Intn(keySpace)
}

func formatKey(idx int, keySize int) string {
	if keySize <= 0 {
		keySize = 8
	}
	key := make([]byte, keySize)
	if keySize >= 8 {
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(idx))
		return string(key)
	}
	binary.PutVarint(key, int64(idx))
	return string(key)
}
