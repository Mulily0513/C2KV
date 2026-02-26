#!/usr/bin/env bash
set -euo pipefail

C2KV_DIR="${C2KV_DIR:-./var/bench-results/c2kv-write}"
ETCD_DIR="${ETCD_DIR:-./var/bench-results/etcd-write}"
OUT_DIR="${OUT_DIR:-./var/bench-results/compare-write}"
MIN_RPS_GAIN="${MIN_RPS_GAIN:-10}"
MIN_P99_GAIN="${MIN_P99_GAIN:-10}"

mkdir -p "$OUT_DIR"

RAW_CSV="$OUT_DIR/per_run.csv"
AGG_CSV="$OUT_DIR/aggregate.csv"
REPORT_MD="$OUT_DIR/report.md"

echo "tag,val_size,clients,rate,run,c2kv_rps,etcd_rps,rps_gain_pct,c2kv_p99_ms,etcd_p99_ms,p99_gain_pct" >"$RAW_CSV"

for c2kv_json in "$C2KV_DIR"/val*_cli*_rate*_run*.json; do
  [[ -f "$c2kv_json" ]] || continue
  tag="$(basename "$c2kv_json" .json)"
  etcd_json="$ETCD_DIR/${tag}.json"
  if [[ ! -f "$etcd_json" ]]; then
    echo "skip missing etcd result: $etcd_json" >&2
    continue
  fi

  meta="$(echo "$tag" | sed -E 's/val([0-9]+)_cli([0-9]+)_rate([0-9]+)_run([0-9]+)/\1,\2,\3,\4/')"
  c2kv_rps="$(jq -r '.summary.requests_per_sec // 0' "$c2kv_json")"
  etcd_rps="$(jq -r '.summary.requests_per_sec // 0' "$etcd_json")"
  c2kv_p99="$(jq -r '.summary.p99_ms // 0' "$c2kv_json")"
  etcd_p99="$(jq -r '.summary.p99_ms // 0' "$etcd_json")"

  rps_gain="$(awk "BEGIN { if (${etcd_rps} == 0) print 0; else printf \"%.4f\", ((${c2kv_rps}-${etcd_rps})/${etcd_rps})*100 }")"
  p99_gain="$(awk "BEGIN { if (${etcd_p99} == 0) print 0; else printf \"%.4f\", ((${etcd_p99}-${c2kv_p99})/${etcd_p99})*100 }")"

  echo "${tag},${meta},${c2kv_rps},${etcd_rps},${rps_gain},${c2kv_p99},${etcd_p99},${p99_gain}" >>"$RAW_CSV"
done

tmp_agg="$(mktemp)"
awk -F, '
NR==1 { next }
{
  key=$2","$3","$4
  cnt[key]++
  c2kv_rps[key]+=$6
  etcd_rps[key]+=$7
  rps_gain[key]+=$8
  c2kv_p99[key]+=$9
  etcd_p99[key]+=$10
  p99_gain[key]+=$11
}
END {
  print "val_size,clients,rate,repeats,avg_c2kv_rps,avg_etcd_rps,avg_rps_gain_pct,avg_c2kv_p99_ms,avg_etcd_p99_ms,avg_p99_gain_pct"
  for (k in cnt) {
    printf "%s,%d,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f\n",
      k, cnt[k],
      c2kv_rps[k]/cnt[k], etcd_rps[k]/cnt[k], rps_gain[k]/cnt[k],
      c2kv_p99[k]/cnt[k], etcd_p99[k]/cnt[k], p99_gain[k]/cnt[k]
  }
}
' "$RAW_CSV" >"$tmp_agg"
{
  head -n1 "$tmp_agg"
  tail -n +2 "$tmp_agg" | sort
} >"$AGG_CSV"
rm -f "$tmp_agg"

{
  echo "# C2KV vs etcd Write Benchmark"
  echo
  echo "- C2KV result dir: \`$C2KV_DIR\`"
  echo "- etcd result dir: \`$ETCD_DIR\`"
  echo "- Thresholds:"
  echo "  - throughput gain >= ${MIN_RPS_GAIN}%"
  echo "  - p99 latency gain >= ${MIN_P99_GAIN}%"
  echo
  echo "## Aggregate"
  echo
  echo "| val_size | clients | rate | repeats | c2kv_rps | etcd_rps | rps_gain% | c2kv_p99_ms | etcd_p99_ms | p99_gain% | pass |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|"

  tail -n +2 "$AGG_CSV" | while IFS=, read -r val clients rate repeats c2kv_rps etcd_rps rps_gain c2kv_p99 etcd_p99 p99_gain; do
    pass="no"
    if awk "BEGIN {exit !(${rps_gain} >= ${MIN_RPS_GAIN} || ${p99_gain} >= ${MIN_P99_GAIN})}"; then
      pass="yes"
    fi
    printf "| %s | %s | %s | %s | %.2f | %.2f | %.2f | %.3f | %.3f | %.2f | %s |\n" \
      "$val" "$clients" "$rate" "$repeats" "$c2kv_rps" "$etcd_rps" "$rps_gain" "$c2kv_p99" "$etcd_p99" "$p99_gain" "$pass"
  done
  echo
  echo "## Files"
  echo
  echo "- Per-run CSV: \`$RAW_CSV\`"
  echo "- Aggregate CSV: \`$AGG_CSV\`"
} >"$REPORT_MD"

echo "compare report generated:"
echo "  $REPORT_MD"
echo "  $AGG_CSV"
echo "  $RAW_CSV"
