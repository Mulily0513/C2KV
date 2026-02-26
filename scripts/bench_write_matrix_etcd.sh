#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ETCD_VERSION="${ETCD_VERSION:-v3.5.13}"
ETCD_BENCH_BIN="${ETCD_BENCH_BIN:-$(go env GOPATH)/bin/benchmark}"
ENDPOINTS="${ENDPOINTS:-127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379}"
OUT_DIR="${OUT_DIR:-./var/bench-results/etcd-write}"
ENGINE_TAG="${ENGINE_TAG:-etcd}"
CLUSTER_NAME="${CLUSTER_NAME:-etcd-3node}"
TOTAL="${TOTAL:-200000}"
REPEATS="${REPEATS:-5}"
VAL_SIZES="${VAL_SIZES:-16 128 1024}"
CLIENTS="${CLIENTS:-1 16 64}"
RATES="${RATES:-0 50000}"

mkdir -p "$OUT_DIR"

ensure_etcd_benchmark() {
  if [[ -x "$ETCD_BENCH_BIN" ]]; then
    return 0
  fi
  echo "installing etcd benchmark ${ETCD_VERSION}..."
  local gobin
  gobin="$(dirname "$ETCD_BENCH_BIN")"
  mkdir -p "$gobin"
  GOBIN="$gobin" go install "go.etcd.io/etcd/tools/benchmark@${ETCD_VERSION}"
}

ensure_etcd_benchmark

for val in $VAL_SIZES; do
  for cli in $CLIENTS; do
    for rate in $RATES; do
      for run in $(seq 1 "$REPEATS"); do
        tag="val${val}_cli${cli}_rate${rate}_run${run}"
        raw="$OUT_DIR/${tag}.txt"
        json="$OUT_DIR/${tag}.json"
        csv="$OUT_DIR/${tag}.csv"

        echo ">>> running ${tag}"
        "$ETCD_BENCH_BIN" put \
          --endpoints="${ENDPOINTS}" \
          --clients="${cli}" \
          --total="${TOTAL}" \
          --val-size="${val}" \
          --rate="${rate}" \
          --target-leader \
          --sample >"$raw"

        ./scripts/bench_parse_output.sh \
          --input "$raw" \
          --json "$json" \
          --csv "$csv" \
          --name "put" \
          --engine-tag "$ENGINE_TAG" \
          --cluster-name "$CLUSTER_NAME" \
          --endpoints "$ENDPOINTS" \
          --requests-total "$TOTAL"
      done
    done
  done
done

echo "all etcd benchmark runs finished: ${OUT_DIR}"
