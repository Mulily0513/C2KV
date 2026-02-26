#!/usr/bin/env bash
set -eo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNNER_DIR="$ROOT_DIR/eval/ycsb-runner"

WORKLOAD="${WORKLOAD:-workloads/workloada}"
THREADS="${THREADS:-16}"
RECORD_COUNT="${RECORD_COUNT:-10000}"
OP_COUNT="${OP_COUNT:-20000}"
LOAD_EXTRA_KEYS="${LOAD_EXTRA_KEYS:-1}"
TARGET="${TARGET:-0}"
INTERVAL="${INTERVAL:-10}"
REPEATS="${REPEATS:-5}"
SKIP_LOAD="${SKIP_LOAD:-0}"
READ_PROPORTION="${READ_PROPORTION:-}"
UPDATE_PROPORTION="${UPDATE_PROPORTION:-}"
SETTLE_SEC="${SETTLE_SEC:-3}"
WAIT_READY_TIMEOUT_SEC="${WAIT_READY_TIMEOUT_SEC:-30}"

C2KV_ENDPOINTS="${C2KV_ENDPOINTS:-127.0.0.1:2341,127.0.0.1:2342,127.0.0.1:2343}"
C2KV_DIAL_TIMEOUT="${C2KV_DIAL_TIMEOUT:-2s}"
C2KV_REQUEST_TIMEOUT="${C2KV_REQUEST_TIMEOUT:-5s}"

ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379}"
ETCD_DIAL_TIMEOUT="${ETCD_DIAL_TIMEOUT:-2s}"

OUT_ROOT="${OUT_ROOT:-$ROOT_DIR/var/bench-results/ycsb-compare}"
RUN_TAG="${RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
OUT_DIR="$OUT_ROOT/$RUN_TAG"
RAW_DIR="$OUT_DIR/raw"
LOG_DIR="$OUT_DIR/logs"
mkdir -p "$RAW_DIR" "$LOG_DIR"

RUNNER_BIN="${RUNNER_BIN:-$OUT_DIR/go-ycsb-runner}"
ETCD_RUNNER_BIN="${ETCD_RUNNER_BIN:-$OUT_DIR/go-ycsb-etcd}"
(cd "$RUNNER_DIR" && go build -o "$RUNNER_BIN" .)
(cd "$RUNNER_DIR" && go build -o "$ETCD_RUNNER_BIN" ./cmd/go-ycsb-etcd)

mix_args=()
if [[ -n "$READ_PROPORTION" ]]; then
  mix_args+=(-p "readproportion=$READ_PROPORTION")
fi
if [[ -n "$UPDATE_PROPORTION" ]]; then
  mix_args+=(-p "updateproportion=$UPDATE_PROPORTION")
fi

run_engine() {
  local engine="$1"
  local endpoints="$2"
  local run="$3"
  local keyprefix="bench_${engine}_r${run}"

  echo ">>> ${engine} run=${run}"

  if [[ "$SKIP_LOAD" != "1" ]]; then
    local load_operation_count=$((RECORD_COUNT + LOAD_EXTRA_KEYS))
    if (( load_operation_count < RECORD_COUNT )); then
      load_operation_count="$RECORD_COUNT"
    fi
    local load_log="$LOG_DIR/${engine}_load${run}.log"
    if [[ "$engine" == "c2kv" ]]; then
      (
        cd "$RUNNER_DIR"
        "$RUNNER_BIN" load "$engine" \
          -P "$WORKLOAD" \
          -p outputstyle=json \
          -p measurementtype=histogram \
          -p measurement.interval="$INTERVAL" \
          -p threadcount="$THREADS" \
          -p target="$TARGET" \
          -p recordcount="$RECORD_COUNT" \
          -p operationcount="$load_operation_count" \
          -p keyprefix="$keyprefix" \
          "${mix_args[@]}" \
          -p c2kv.endpoints="$endpoints" \
          -p c2kv.dial_timeout="$C2KV_DIAL_TIMEOUT" \
          -p c2kv.request_timeout="$C2KV_REQUEST_TIMEOUT"
      ) | tee "$load_log"
    else
      (
        cd "$RUNNER_DIR"
        "$ETCD_RUNNER_BIN" load "$engine" \
          -P "$WORKLOAD" \
          -p outputstyle=json \
          -p measurementtype=histogram \
          -p measurement.interval="$INTERVAL" \
          -p threadcount="$THREADS" \
          -p target="$TARGET" \
          -p recordcount="$RECORD_COUNT" \
          -p operationcount="$load_operation_count" \
          -p keyprefix="$keyprefix" \
          "${mix_args[@]}" \
          -p etcd.endpoints="$endpoints" \
          -p etcd.dial_timeout="$ETCD_DIAL_TIMEOUT"
      ) | tee "$load_log"
    fi
  fi

  local run_log="$LOG_DIR/${engine}_run${run}.log"
  local run_json="$RAW_DIR/${engine}_run${run}.json"
  if [[ "$engine" == "c2kv" ]]; then
    (
      cd "$RUNNER_DIR"
      "$RUNNER_BIN" run "$engine" \
        -P "$WORKLOAD" \
        -p outputstyle=json \
        -p measurementtype=histogram \
        -p measurement.interval="$INTERVAL" \
        -p measurement.output_file="$run_json" \
        -p threadcount="$THREADS" \
        -p target="$TARGET" \
        -p recordcount="$RECORD_COUNT" \
        -p operationcount="$OP_COUNT" \
        -p keyprefix="$keyprefix" \
        "${mix_args[@]}" \
        -p c2kv.endpoints="$endpoints" \
        -p c2kv.dial_timeout="$C2KV_DIAL_TIMEOUT" \
        -p c2kv.request_timeout="$C2KV_REQUEST_TIMEOUT"
    ) | tee "$run_log"
  else
    (
      cd "$RUNNER_DIR"
      "$ETCD_RUNNER_BIN" run "$engine" \
        -P "$WORKLOAD" \
        -p outputstyle=json \
        -p measurementtype=histogram \
        -p measurement.interval="$INTERVAL" \
        -p measurement.output_file="$run_json" \
        -p threadcount="$THREADS" \
        -p target="$TARGET" \
        -p recordcount="$RECORD_COUNT" \
        -p operationcount="$OP_COUNT" \
        -p keyprefix="$keyprefix" \
        "${mix_args[@]}" \
        -p etcd.endpoints="$endpoints" \
        -p etcd.dial_timeout="$ETCD_DIAL_TIMEOUT"
    ) | tee "$run_log"
  fi
}

wait_for_endpoint() {
  local endpoint="$1"
  local host="${endpoint%%:*}"
  local port="${endpoint##*:}"
  local deadline=$((SECONDS + WAIT_READY_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "endpoint not ready within ${WAIT_READY_TIMEOUT_SEC}s: ${endpoint}" >&2
  return 1
}

wait_for_endpoints() {
  local endpoints="$1"
  IFS=',' read -r -a eps <<<"$endpoints"
  for ep in "${eps[@]}"; do
    wait_for_endpoint "$ep"
  done
}

echo "output dir: $OUT_DIR"
echo "workload:   $WORKLOAD"
echo "threads:    $THREADS"
echo "recordcount:$RECORD_COUNT"
echo "opcount:    $OP_COUNT"
echo "load-extra: $LOAD_EXTRA_KEYS"
echo "repeats:    $REPEATS"
if [[ -n "$READ_PROPORTION" || -n "$UPDATE_PROPORTION" ]]; then
  echo "mix:        read=$READ_PROPORTION update=$UPDATE_PROPORTION"
fi

for run in $(seq 1 "$REPEATS"); do
  if (( run % 2 == 1 )); then
    wait_for_endpoints "$C2KV_ENDPOINTS"
    wait_for_endpoints "$ETCD_ENDPOINTS"
    if (( SETTLE_SEC > 0 )); then sleep "$SETTLE_SEC"; fi
    run_engine "c2kv" "$C2KV_ENDPOINTS" "$run"
    run_engine "etcd" "$ETCD_ENDPOINTS" "$run"
  else
    wait_for_endpoints "$C2KV_ENDPOINTS"
    wait_for_endpoints "$ETCD_ENDPOINTS"
    if (( SETTLE_SEC > 0 )); then sleep "$SETTLE_SEC"; fi
    run_engine "etcd" "$ETCD_ENDPOINTS" "$run"
    run_engine "c2kv" "$C2KV_ENDPOINTS" "$run"
  fi
done

python3 "$RUNNER_DIR/scripts/summarize_compare.py" \
  --input-dir "$RAW_DIR" \
  --output-csv "$OUT_DIR/summary.csv" \
  --output-md "$OUT_DIR/report.md"

echo
echo "done."
echo "summary csv: $OUT_DIR/summary.csv"
echo "report md:   $OUT_DIR/report.md"
