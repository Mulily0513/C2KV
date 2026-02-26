# go-ycsb runner for C2KV / etcd

This module uses upstream `github.com/pingcap/go-ycsb` workload/metrics engine and supports:

- `c2kv` (custom driver in this repo, default binary `go run .`)
- `etcd` (isolated binary `go run ./cmd/go-ycsb-etcd`)

## Quick start

1. Ensure a C2KV cluster is up (3 nodes in `deploy/dev`).
2. Run:

```bash
cd eval/ycsb-runner
./scripts/run_workloada.sh
```

## Manual commands

```bash
cd eval/ycsb-runner

go run . load c2kv \
  -P workloads/workloada \
  -p c2kv.endpoints=127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343

go run . run c2kv \
  -P workloads/workloada \
  -p c2kv.endpoints=127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343

go run ./cmd/go-ycsb-etcd load etcd \
  -P workloads/workloada \
  -p etcd.endpoints=127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379

go run ./cmd/go-ycsb-etcd run etcd \
  -P workloads/workloada \
  -p etcd.endpoints=127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379
```

## Supported properties

- `c2kv.endpoints` (default `127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343`)
- `c2kv.dial_timeout` (default `2s`)
- `c2kv.request_timeout` (default `5s`)
- `etcd.endpoints` (default `localhost:2379`)
- `etcd.dial_timeout` (default `2s`)

## C2KV vs etcd compare

Use the compare script to run repeated YCSB tests and export a report:

```bash
cd eval/ycsb-runner
./scripts/run_compare_c2kv_etcd.sh
```

For fairness, run c2kv and etcd with the same node count (3), CPU, memory, disk type, and network limits.

Key env vars:

- `C2KV_ENDPOINTS`, `ETCD_ENDPOINTS`
- `THREADS`, `RECORD_COUNT`, `OP_COUNT`, `REPEATS`
- `OUT_ROOT`, `RUN_TAG`

Outputs:

- `var/bench-results/ycsb-compare/<run-tag>/summary.csv`
- `var/bench-results/ycsb-compare/<run-tag>/report.md`
