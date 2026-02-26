# C2KV Benchmark

`eval/benchmark` is a low-level benchmark tool adapted from etcd benchmark style.

## Commands

- `put`: write benchmark
- `range`: point/range read benchmark
- `mixed`: mixed read/write benchmark

## Common flags

```sh
--endpoints=127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343
--clients=16
--target-leader
--output-json=./bench/result.json
--output-csv=./bench/timeseries.csv
--cluster-name=c2kv-dev
--engine-tag=c2kv
```

## Engine stage metrics

When endpoints are C2KV nodes, benchmark also collects stage metrics (cluster delta):

- `propose_wait`
- `wal_write`
- `wal_sync`
- `apply`

These metrics are exported in JSON under `engine_stats` with `count / total_ms / avg_ms`.

## Examples

```sh
# put benchmark
go run ./eval/benchmark put \
  --endpoints=127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343 \
  --clients=64 \
  --total=200000 \
  --val-size=128 \
  --target-leader \
  --output-json=./bench/put.json \
  --output-csv=./bench/put.csv

# range benchmark (serializable local read)
go run ./eval/benchmark range \
  --endpoints=127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343 \
  --total=200000 \
  --serializable \
  --range-size=1

# mixed benchmark (50% read / 50% write)
go run ./eval/benchmark mixed \
  --endpoints=127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343 \
  --total=300000 \
  --read-percent=50 \
  --warmup-writes=20000 \
  --target-leader \
  --output-json=./bench/mixed.json
```

## etcd 对比流程（v3.5.13）

```sh
# 1) 跑 C2KV 写入矩阵
./scripts/bench_write_matrix.sh

# 2) 跑 etcd 写入矩阵（自动安装 benchmark@v3.5.13）
./scripts/bench_write_matrix_etcd.sh

# 3) 生成对比报告
./scripts/bench_compare_write.sh
```

常用输出目录：

- `./var/bench-results/c2kv-write`
- `./var/bench-results/etcd-write`
- `./var/bench-results/compare-write/report.md`
