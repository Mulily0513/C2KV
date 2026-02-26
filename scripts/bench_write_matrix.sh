#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENDPOINTS="${ENDPOINTS:-127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343}"
OUT_DIR="${OUT_DIR:-./var/bench-results/c2kv-write}"
ENGINE_TAG="${ENGINE_TAG:-c2kv}"
CLUSTER_NAME="${CLUSTER_NAME:-c2kv-3node}"
TOTAL="${TOTAL:-200000}"
REPEATS="${REPEATS:-5}"
VAL_SIZES="${VAL_SIZES:-16 128 1024}"
CLIENTS="${CLIENTS:-1 16 64}"
RATES="${RATES:-0 50000}"

mkdir -p "$OUT_DIR"

for val in $VAL_SIZES; do
  for cli in $CLIENTS; do
    for rate in $RATES; do
      for run in $(seq 1 "$REPEATS"); do
        tag="val${val}_cli${cli}_rate${rate}_run${run}"
        json="$OUT_DIR/${tag}.json"
        csv="$OUT_DIR/${tag}.csv"
        echo ">>> running ${tag}"
        go run ./eval/benchmark put \
          --endpoints="${ENDPOINTS}" \
          --clients="${cli}" \
          --total="${TOTAL}" \
          --val-size="${val}" \
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

echo "all benchmark runs finished: ${OUT_DIR}"
