#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNNER_DIR="$ROOT_DIR/tools/ycsb-runner"

ENDPOINTS="${ENDPOINTS:-127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343}"
THREADS="${THREADS:-16}"
RECORD_COUNT="${RECORD_COUNT:-10000}"
OP_COUNT="${OP_COUNT:-20000}"
REQ_TIMEOUT="${REQ_TIMEOUT:-5s}"

cd "$RUNNER_DIR"

echo "[1/2] load phase"
go run . load c2kv \
  -P workloads/workloada \
  -p c2kv.endpoints="$ENDPOINTS" \
  -p c2kv.request_timeout="$REQ_TIMEOUT" \
  -p threadcount="$THREADS" \
  -p recordcount="$RECORD_COUNT" \
  -p operationcount="$RECORD_COUNT"

echo "[2/2] run phase"
go run . run c2kv \
  -P workloads/workloada \
  -p c2kv.endpoints="$ENDPOINTS" \
  -p c2kv.request_timeout="$REQ_TIMEOUT" \
  -p threadcount="$THREADS" \
  -p recordcount="$RECORD_COUNT" \
  -p operationcount="$OP_COUNT"
