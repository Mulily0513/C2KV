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
	"crypto/rand"
	"fmt"
	"github.com/Mulily0513/C2KV/client"
	"github.com/Mulily0513/C2KV/pkg/report"
	"google.golang.org/grpc/grpclog"
	"os"
)

var (
	// dialTotal counts the number of mustCreateConn calls so that endpoint
	// connections can be handed out in round-robin order
	dialTotal int

	// leaderEps is a cache for holding endpoints of a leader node
	leaderEps []string

	// cache the username and password for multiple connections
	globalUserName string
	globalPassword string
)

func mustFindLeaderEndpoints(c *client.Client) {
	resp, lerr := c.MemberList(context.TODO())
	if lerr != nil {
		fmt.Fprintf(os.Stderr, "failed to get a member list: %s\n", lerr)
		os.Exit(1)
	}

	for _, m := range resp.Members {
		if m.IsLeader {
			leaderEps = m.ClientURLs
			return
		}
	}

	fmt.Fprintf(os.Stderr, "failed to find a leader endpoint\n")
	os.Exit(1)
}

func mustCreateConn() *client.Client {
	connEndpoints := leaderEps
	if len(connEndpoints) == 0 {
		connEndpoints = []string{endpoints[dialTotal%len(endpoints)]}
		dialTotal++
	}

	cfg := client.Config{
		Endpoints:   connEndpoints,
		DialTimeout: dialTimeout,
	}

	client, err := client.New(cfg)
	if targetLeader && len(leaderEps) == 0 {
		mustFindLeaderEndpoints(client)
		client.Close()
		return mustCreateConn()
	}

	grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))

	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}

	return client
}

// 在某些场景下，可能由于资源限制（如数据库连接数限制、API 调用频率限制等）而无法为每个客户端都提供独立的连接。通过共享连接，可以最大化现有资源的利用率
// 假设我们想要创建总共3个客户端（totalClients = 3），并且我们只有2个可用的连接（totalConns = 2）。这意味着，虽然我们想要3个客户端，但我们只能提供2个实际的网络连接。下面的步骤说明了 mustCreateClients 函数如何分配这些连接给客户端：
//
// 首先，函数会创建一个长度为2的连接切片 conns，然后通过调用 mustCreateConn() 函数两次来填充这个切片。假设 mustCreateConn() 成功创建了两个连接，我们可以称它们为 conn1 和 conn2。
//
// 接下来，函数会创建一个长度为3的客户端切片 clients。
//
// 然后，函数进入一个循环，为每个客户端分配一个连接。由于我们只有2个连接，所以我们将循环使用它们。分配情况如下：
//
// clients[0] 被分配 conn1（conns[0]）。
// clients[1] 被分配 conn2（conns[1]）。
// clients[2] 再次被分配 conn1，因为 2 % 2 的结果是0，这意味着它循环回到 conns 切片的第一个元素。
// 最终，我们得到了3个客户端，但它们共享2个连接。具体来说：
//
// clients[0] 和 clients[2] 共享第一个连接 conn1。
// clients[1] 使用第二个连接 conn2。
// 函数返回填充了连接的 clients 切片。
func mustCreateClients(totalClients, totalConns uint) []*client.Client {
	conns := make([]*client.Client, totalConns)
	for i := range conns {
		conns[i] = mustCreateConn()
	}

	clients := make([]*client.Client, totalClients)
	for i := range clients {
		clients[i] = conns[i%int(totalConns)]
	}
	return clients
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

func newWeightedReport() report.Report {
	p := "%4.4f"
	if precise {
		p = "%g"
	}
	if sample {
		return report.NewReportSample(p)
	}
	return report.NewWeightedReport(report.NewReport(p), p)
}
