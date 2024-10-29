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
	"fmt"
	"github.com/Mulily0513/C2KV/client"
	"github.com/Mulily0513/C2KV/tools/report"
	"os"
)

func mustCreatLeaderClients(totalClient int, endpoints []string) ([]*client.Client, error) {
	conns := make([]*client.Client, totalClient)
	cfg := client.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	}

	leaderClient, s, err := client.FindLeaderClient(cfg)
	if err != nil {
		return nil, err
	}
	conns = append(conns, leaderClient)

	if totalClient == 1 {
		return conns, nil
	}

	for i := 1; i < totalClient; i++ {
		client, err := client.NewClient(s)
		if err != nil {
			return nil, err
		}
		conns = append(conns, client)
	}
	return conns, nil
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
	if sample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}
