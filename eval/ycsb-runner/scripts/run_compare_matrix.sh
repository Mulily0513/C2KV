#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNNER_DIR="$ROOT_DIR/eval/ycsb-runner"

CLIENTS_LIST="${CLIENTS_LIST:-1 16 64}"
VAL_SIZES="${VAL_SIZES:-16 128 1024}"
REPEATS="${REPEATS:-5}"
RECORD_COUNT="${RECORD_COUNT:-2000}"
OP_COUNT="${OP_COUNT:-4000}"
TARGET="${TARGET:-0}"
INTERVAL="${INTERVAL:-5}"
MATRIX_TAG="${MATRIX_TAG:-matrix-$(date +%Y%m%d-%H%M%S)}"
OUT_ROOT="${OUT_ROOT:-$ROOT_DIR/var/bench-results/ycsb-matrix}"

MATRIX_DIR="$OUT_ROOT/$MATRIX_TAG"
mkdir -p "$MATRIX_DIR"
SUMMARY_CSV="$MATRIX_DIR/matrix_summary.csv"

echo "clients,val_size,repeats,run_tag,c2kv_total_ops,etcd_total_ops,total_gain_pct,c2kv_update_ops,etcd_update_ops,update_gain_pct,c2kv_update_p99_us,etcd_update_p99_us,update_p99_improve_pct,c2kv_error_rate_pct,etcd_error_rate_pct" >"$SUMMARY_CSV"

workload_for_val() {
  case "$1" in
  16) echo "workloads/workloada_v16" ;;
  128) echo "workloads/workloada_v128" ;;
  1024) echo "workloads/workloada_v1024" ;;
  *)
    echo "unsupported val size: $1" >&2
    exit 1
    ;;
  esac
}

summarize_run() {
  local clients="$1"
  local val="$2"
  local run_tag="$3"
  local run_dir="$ROOT_DIR/var/bench-results/ycsb-compare/$run_tag"
  local summary_csv="$run_dir/summary.csv"
  local report_md="$run_dir/report.md"

  python3 - "$clients" "$val" "$REPEATS" "$run_tag" "$summary_csv" "$report_md" >>"$SUMMARY_CSV" <<'PY'
import csv, re, sys
from collections import defaultdict

clients, val, repeats, run_tag, summary_path, report_path = sys.argv[1:]

stats = defaultdict(list)
with open(summary_path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row["engine"], row["operation"])
        stats[key].append(row)

def mean_float(values):
    if not values:
        return 0.0
    return sum(values) / len(values)

def mean_metric(engine, op, field):
    rows = stats.get((engine, op), [])
    vals = [float(r[field]) for r in rows if r.get(field) not in (None, "")]
    return mean_float(vals)

c2kv_total = mean_metric("c2kv", "TOTAL", "ops")
etcd_total = mean_metric("etcd", "TOTAL", "ops")
c2kv_update = mean_metric("c2kv", "UPDATE", "ops")
etcd_update = mean_metric("etcd", "UPDATE", "ops")
c2kv_update_p99 = mean_metric("c2kv", "UPDATE", "p99_us")
etcd_update_p99 = mean_metric("etcd", "UPDATE", "p99_us")

total_gain = ((c2kv_total / etcd_total) - 1.0) * 100.0 if etcd_total > 0 else 0.0
update_gain = ((c2kv_update / etcd_update) - 1.0) * 100.0 if etcd_update > 0 else 0.0
update_p99_improve = (1.0 - c2kv_update_p99 / etcd_update_p99) * 100.0 if etcd_update_p99 > 0 else 0.0

c2kv_err = 0.0
etcd_err = 0.0
with open(report_path, "r", encoding="utf-8") as f:
    in_rel = False
    for line in f:
        if line.strip() == "## Reliability Summary (mean across runs)":
            in_rel = True
            continue
        if in_rel and line.startswith("## "):
            break
        if in_rel and line.startswith("| c2kv "):
            m = re.search(r"\|\s*([0-9.]+)%\s*\|", line)
            if m:
                c2kv_err = float(m.group(1))
        if in_rel and line.startswith("| etcd "):
            m = re.search(r"\|\s*([0-9.]+)%\s*\|", line)
            if m:
                etcd_err = float(m.group(1))

print(
    f"{clients},{val},{repeats},{run_tag},"
    f"{c2kv_total:.2f},{etcd_total:.2f},{total_gain:.2f},"
    f"{c2kv_update:.2f},{etcd_update:.2f},{update_gain:.2f},"
    f"{c2kv_update_p99:.2f},{etcd_update_p99:.2f},{update_p99_improve:.2f},"
    f"{c2kv_err:.4f},{etcd_err:.4f}"
)
PY
}

echo "matrix tag:  $MATRIX_TAG"
echo "matrix dir:  $MATRIX_DIR"
echo "clients:     $CLIENTS_LIST"
echo "val sizes:   $VAL_SIZES"
echo "repeats:     $REPEATS"
echo "recordcount: $RECORD_COUNT"
echo "opcount:     $OP_COUNT"

for clients in $CLIENTS_LIST; do
  for val in $VAL_SIZES; do
    workload="$(workload_for_val "$val")"
    run_tag="${MATRIX_TAG}-c${clients}-v${val}"
    echo
    echo "=== run $run_tag ==="
    (
      cd "$RUNNER_DIR"
      WORKLOAD="$workload" \
      THREADS="$clients" \
      REPEATS="$REPEATS" \
      RECORD_COUNT="$RECORD_COUNT" \
      OP_COUNT="$OP_COUNT" \
      TARGET="$TARGET" \
      INTERVAL="$INTERVAL" \
      RUN_TAG="$run_tag" \
      ./scripts/run_compare_c2kv_etcd.sh
    )
    summarize_run "$clients" "$val" "$run_tag"
  done
done

echo
echo "matrix done."
echo "summary: $SUMMARY_CSV"
