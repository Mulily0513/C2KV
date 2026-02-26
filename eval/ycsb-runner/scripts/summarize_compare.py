#!/usr/bin/env python3
import argparse
import csv
import glob
import json
import os
import re
import statistics
from collections import defaultdict


FILE_PATTERN = re.compile(r"^(?P<engine>[a-zA-Z0-9_-]+)_run(?P<run>[0-9]+)\.json$")


def to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, (float, int)):
        return float(value)
    value = str(value).strip()
    if value == "":
        return 0.0
    return float(value)


def read_rows(input_dir):
    rows = []
    for path in sorted(glob.glob(os.path.join(input_dir, "*.json"))):
        base = os.path.basename(path)
        match = FILE_PATTERN.match(base)
        if not match:
            continue
        engine = match.group("engine")
        run = int(match.group("run"))
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            continue
        for item in data:
            op = str(item.get("Operation", "")).upper()
            if op == "":
                continue
            rows.append(
                {
                    "engine": engine,
                    "run": run,
                    "operation": op,
                    "takes_s": to_float(item.get("Takes(s)")),
                    "count": to_float(item.get("Count")),
                    "ops": to_float(item.get("OPS")),
                    "avg_us": to_float(item.get("Avg(us)")),
                    "min_us": to_float(item.get("Min(us)")),
                    "max_us": to_float(item.get("Max(us)")),
                    "p50_us": to_float(item.get("50th(us)")),
                    "p90_us": to_float(item.get("90th(us)")),
                    "p95_us": to_float(item.get("95th(us)")),
                    "p99_us": to_float(item.get("99th(us)")),
                    "p999_us": to_float(item.get("99.9th(us)")),
                    "p9999_us": to_float(item.get("99.99th(us)")),
                }
            )
    return rows


def aggregate_rows(rows):
    grouped = defaultdict(list)
    for row in rows:
        key = (row["engine"], row["operation"])
        grouped[key].append(row)

    agg = {}
    for key, items in grouped.items():
        ops = [x["ops"] for x in items]
        p99 = [x["p99_us"] for x in items]
        p95 = [x["p95_us"] for x in items]
        p50 = [x["p50_us"] for x in items]
        avg = [x["avg_us"] for x in items]
        agg[key] = {
            "runs": len(items),
            "ops_mean": statistics.mean(ops),
            "ops_stdev": statistics.pstdev(ops) if len(ops) > 1 else 0.0,
            "p50_mean": statistics.mean(p50),
            "p95_mean": statistics.mean(p95),
            "p99_mean": statistics.mean(p99),
            "avg_mean": statistics.mean(avg),
        }
    return agg


def aggregate_runs(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["engine"], row["run"])].append(row)

    out = []
    for (engine, run), items in grouped.items():
        total = None
        error_count = 0.0
        for row in items:
            if row["operation"] == "TOTAL":
                total = row
            if row["operation"].endswith("_ERROR"):
                error_count += row["count"]

        takes_s = total["takes_s"] if total is not None else 0.0
        success_count = total["count"] if total is not None else 0.0
        success_ops = total["ops"] if total is not None else 0.0
        attempted_count = success_count + error_count
        attempted_ops = (attempted_count / takes_s) if takes_s > 0 else 0.0
        error_rate = (error_count / attempted_count) if attempted_count > 0 else 0.0

        out.append(
            {
                "engine": engine,
                "run": run,
                "takes_s": takes_s,
                "success_count": success_count,
                "error_count": error_count,
                "attempted_count": attempted_count,
                "success_ops": success_ops,
                "attempted_ops": attempted_ops,
                "error_rate": error_rate,
            }
        )

    return sorted(out, key=lambda x: (x["engine"], x["run"]))


def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fields = [
        "engine",
        "run",
        "operation",
        "takes_s",
        "count",
        "ops",
        "avg_us",
        "min_us",
        "max_us",
        "p50_us",
        "p90_us",
        "p95_us",
        "p99_us",
        "p999_us",
        "p9999_us",
    ]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in sorted(rows, key=lambda x: (x["engine"], x["run"], x["operation"])):
            writer.writerow(row)


def compare_percent(new, base):
    if base == 0:
        return 0.0
    return (new / base - 1.0) * 100.0


def compare_latency_improvement(new, base):
    if base == 0:
        return 0.0
    return (1.0 - new / base) * 100.0


def write_markdown(path, rows, agg):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    engines = sorted({r["engine"] for r in rows})
    operations = sorted({r["operation"] for r in rows})

    run_stats = aggregate_runs(rows)
    run_stats_by_engine = defaultdict(list)
    for row in run_stats:
        run_stats_by_engine[row["engine"]].append(row)

    with open(path, "w", encoding="utf-8") as fh:
        fh.write("# YCSB Compare Report\n\n")

        fh.write("## Run-Level Reliability\n\n")
        fh.write("| Engine | Run | Success OPS | Attempted OPS | Error Rate | Errors |\n")
        fh.write("|---|---:|---:|---:|---:|---:|\n")
        for row in run_stats:
            fh.write(
                f"| {row['engine']} | {row['run']} | {row['success_ops']:.2f} | {row['attempted_ops']:.2f} | "
                f"{row['error_rate'] * 100:.4f}% | {int(row['error_count'])} |\n"
            )

        fh.write("\n## Reliability Summary (mean across runs)\n\n")
        fh.write("| Engine | Runs | Success OPS Mean | Error Rate Mean |\n")
        fh.write("|---|---:|---:|---:|\n")
        for engine in sorted(run_stats_by_engine.keys()):
            items = run_stats_by_engine[engine]
            fh.write(
                f"| {engine} | {len(items)} | "
                f"{statistics.mean(x['success_ops'] for x in items):.2f} | "
                f"{statistics.mean(x['error_rate'] for x in items) * 100:.4f}% |\n"
            )

        fh.write("\n")
        fh.write("## Aggregated Metrics (mean across runs)\n\n")
        fh.write("| Engine | Operation | Runs | OPS | Avg(us) | P50(us) | P95(us) | P99(us) |\n")
        fh.write("|---|---|---:|---:|---:|---:|---:|---:|\n")
        for engine in engines:
            for op in operations:
                item = agg.get((engine, op))
                if item is None:
                    continue
                fh.write(
                    f"| {engine} | {op} | {item['runs']} | {item['ops_mean']:.2f} | {item['avg_mean']:.2f} | "
                    f"{item['p50_mean']:.2f} | {item['p95_mean']:.2f} | {item['p99_mean']:.2f} |\n"
                )

        if ("c2kv", "READ") in agg and ("etcd", "READ") in agg:
            c = agg[("c2kv", "READ")]
            e = agg[("etcd", "READ")]
            fh.write("\n## READ Comparison (C2KV vs etcd)\n\n")
            fh.write(
                f"- OPS change: {compare_percent(c['ops_mean'], e['ops_mean']):.2f}%\n"
            )
            fh.write(
                f"- P99 latency improvement: {compare_latency_improvement(c['p99_mean'], e['p99_mean']):.2f}%\n"
            )

        if ("c2kv", "UPDATE") in agg and ("etcd", "UPDATE") in agg:
            c = agg[("c2kv", "UPDATE")]
            e = agg[("etcd", "UPDATE")]
            fh.write("\n## UPDATE Comparison (C2KV vs etcd)\n\n")
            fh.write(
                f"- OPS change: {compare_percent(c['ops_mean'], e['ops_mean']):.2f}%\n"
            )
            fh.write(
                f"- P99 latency improvement: {compare_latency_improvement(c['p99_mean'], e['p99_mean']):.2f}%\n"
            )


def main():
    parser = argparse.ArgumentParser(description="Summarize YCSB engine comparison outputs.")
    parser.add_argument("--input-dir", required=True, help="Directory of *_run*.json raw outputs.")
    parser.add_argument("--output-csv", required=True, help="Path to write run-level CSV.")
    parser.add_argument("--output-md", required=True, help="Path to write markdown summary.")
    args = parser.parse_args()

    rows = read_rows(args.input_dir)
    if not rows:
        raise SystemExit(f"no valid ycsb json files found in {args.input_dir}")
    agg = aggregate_rows(rows)
    write_csv(args.output_csv, rows)
    write_markdown(args.output_md, rows, agg)


if __name__ == "__main__":
    main()
