package cmd

import (
	"os"
	"testing"
)

func TestPut(t *testing.T) {
	if os.Getenv("C2KV_BENCHMARK_E2E") != "1" {
		t.Skip("set C2KV_BENCHMARK_E2E=1 to run benchmark e2e test")
	}
	endpoints = []string{"172.18.0.11:2345", "172.18.0.12:2345", "172.18.0.13:2345"}
	putFunc(nil, nil)
}
