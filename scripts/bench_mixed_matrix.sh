#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENDPOINTS="${ENDPOINTS:-127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343}"
OUT_DIR="${OUT_DIR:-./var/bench-results/c2kv-mixed}"
ENGINE_TAG="${ENGINE_TAG:-c2kv}"
CLUSTER_NAME="${CLUSTER_NAME:-c2kv-3node}"
TOTAL="${TOTAL:-300000}"
REPEATS="${REPEATS:-5}"
READ_PERCENTS="${READ_PERCENTS:-50 80}"
CLIENTS="${CLIENTS:-16 64}"
RATES="${RATES:-0 50000}"

mkdir -p "$OUT_DIR"

for rp in $READ_PERCENTS; do
  for cli in $CLIENTS; do
    for rate in $RATES; do
      for run in $(seq 1 "$REPEATS"); do
        tag="read${rp}_cli${cli}_rate${rate}_run${run}"
        json="$OUT_DIR/${tag}.json"
        csv="$OUT_DIR/${tag}.csv"
        echo ">>> running ${tag}"
        go run ./eval/benchmark mixed \
          --endpoints="${ENDPOINTS}" \
          --clients="${cli}" \
          --total="${TOTAL}" \
          --read-percent="${rp}" \
          --rate="${rate}" \
          --target-leader \
          --engine-tag="${ENGINE_TAG}" \
          --cluster-name="${CLUSTER_NAME}" \
          --output-json="${json}" \
          --output-csv="${csv}"
      done
    done
  done
done

echo "all mixed benchmark runs finished: ${OUT_DIR}"
