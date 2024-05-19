package cmd

import "testing"

func TestPut(t *testing.T) {
	endpoints = []string{"172.18.0.11:2345", "172.18.0.12:2345", "172.18.0.13:2345"}
	putFunc(nil, nil)
}
