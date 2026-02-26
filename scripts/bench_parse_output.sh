#!/usr/bin/env bash
set -euo pipefail

input=""
json_out=""
csv_out=""
name=""
engine_tag=""
cluster_name=""
endpoints=""
requests_total=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input) input="$2"; shift 2 ;;
    --json) json_out="$2"; shift 2 ;;
    --csv) csv_out="$2"; shift 2 ;;
    --name) name="$2"; shift 2 ;;
    --engine-tag) engine_tag="$2"; shift 2 ;;
    --cluster-name) cluster_name="$2"; shift 2 ;;
    --endpoints) endpoints="$2"; shift 2 ;;
    --requests-total) requests_total="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$input" || -z "$json_out" ]]; then
  echo "usage: $0 --input FILE --json FILE [--csv FILE --name NAME --engine-tag TAG --cluster-name NAME --endpoints CSV --requests-total N]" >&2
  exit 1
fi

if [[ ! -f "$input" ]]; then
  echo "input not found: $input" >&2
  exit 1
fi

extract_metric() {
  local key="$1"
  local line
  line="$(grep -E "$key" "$input" | head -n1 || true)"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  if [[ "$line" == *:* ]]; then
    echo "$line" | sed -E 's/^[^:]*:[[:space:]]*([0-9]+(\.[0-9]+)?).*/\1/'
  else
    echo "$line" | grep -Eo '[0-9]+(\.[0-9]+)?' | head -n1
  fi
}

extract_latency_pct_ms() {
  local pct="$1"
  local line
  line="$(grep -E "^[[:space:]]*${pct}% in " "$input" | head -n1 || true)"
  if [[ -z "$line" ]]; then
    echo "0"
    return
  fi
  local secs
  secs="$(echo "$line" | sed -E 's/.* in ([0-9]+(\.[0-9]+)?) secs\..*/\1/')"
  awk "BEGIN { printf \"%.6f\", ${secs} * 1000 }"
}

total_seconds="$(extract_metric '^[[:space:]]*Total:')"
slowest_seconds="$(extract_metric '^[[:space:]]*Slowest:')"
fastest_seconds="$(extract_metric '^[[:space:]]*Fastest:')"
average_seconds="$(extract_metric '^[[:space:]]*Average:')"
rps="$(extract_metric 'Requests/sec:')"
p50_ms="$(extract_latency_pct_ms 50)"
p95_ms="$(extract_latency_pct_ms 95)"
p99_ms="$(extract_latency_pct_ms 99)"

success_requests=0
if [[ "$requests_total" =~ ^[0-9]+$ && "$requests_total" -gt 0 ]]; then
  success_requests="$requests_total"
fi

mkdir -p "$(dirname "$json_out")"
jq -n \
  --arg name "$name" \
  --arg cluster_name "$cluster_name" \
  --arg engine_tag "$engine_tag" \
  --arg endpoints "$endpoints" \
  --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --argjson total_requests "$requests_total" \
  --argjson success_requests "$success_requests" \
  --argjson failed_requests 0 \
  --argjson error_rate 0 \
  --argjson timeout_rate 0 \
  --argjson requests_per_sec "$rps" \
  --argjson total_seconds "$total_seconds" \
  --argjson fastest_ms "$(awk "BEGIN { printf \"%.6f\", ${fastest_seconds} * 1000 }")" \
  --argjson average_ms "$(awk "BEGIN { printf \"%.6f\", ${average_seconds} * 1000 }")" \
  --argjson p50_ms "$p50_ms" \
  --argjson p95_ms "$p95_ms" \
  --argjson p99_ms "$p99_ms" \
  --argjson slowest_ms "$(awk "BEGIN { printf \"%.6f\", ${slowest_seconds} * 1000 }")" \
  '
{
  name: $name,
  cluster_name: $cluster_name,
  engine_tag: $engine_tag,
  timestamp: $ts,
  endpoints: ($endpoints | split(",")),
  summary: {
    total_requests: $total_requests,
    success_requests: $success_requests,
    failed_requests: $failed_requests,
    error_rate: $error_rate,
    timeout_rate: $timeout_rate,
    requests_per_sec: $requests_per_sec,
    total_seconds: $total_seconds,
    fastest_ms: $fastest_ms,
    average_ms: $average_ms,
    p50_ms: $p50_ms,
    p95_ms: $p95_ms,
    p99_ms: $p99_ms,
    slowest_ms: $slowest_ms
  }
}
' >"$json_out"

if [[ -n "$csv_out" ]]; then
  mkdir -p "$(dirname "$csv_out")"
  awk '
    BEGIN { in_series=0 }
    /^UNIX-SECOND,MIN-LATENCY-MS,AVG-LATENCY-MS,MAX-LATENCY-MS,AVG-THROUGHPUT$/ { in_series=1; print; next }
    in_series==1 {
      if ($0 ~ /^[0-9]+,/) { print; next }
      if (length($0) == 0) { exit }
    }
  ' "$input" >"$csv_out" || true
fi
