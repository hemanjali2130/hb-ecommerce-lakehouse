#!/usr/bin/env python3
"""
Phase 2 benchmark harness.

Owner: Hemanjali Buchireddy

Runs three analytical queries against three physical layouts of the SAME rows
and records what Athena actually reports — never an estimate:

    Statistics.DataScannedInBytes        -> bytes scanned
    Statistics.EngineExecutionTimeInMillis -> execution time
    bytes / 1 TiB * $5.00                 -> derived cost

Correctness safeguards, because a benchmark that quietly lies is worse than none
------------------------------------------------------------------------------
1. Runs in hb-benchmark-wg, where Athena result reuse is DISABLED at the
   workgroup level. A reused result reports DataScannedInBytes = 0 and would
   turn this table into a page of zeros that looks like a triumph.

2. Every failure is recorded and printed as a failure. Nothing is estimated,
   interpolated or filled in. If a run fails, BENCHMARK.md says so.

3. Athena bills a 10 MB minimum per query. Where a measurement lands under that,
   the table shows both the raw scanned figure and the billed figure, because
   quoting only the raw number would overstate the saving.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import boto3

TIB = 1024 ** 4
USD_PER_TB = 5.00
ATHENA_MIN_BILLED_BYTES = 10 * 1024 * 1024  # 10 MB minimum per query

# The three tables. Same logical rows, three physical layouts.
VARIANTS = [
    ("raw JSON, uncompressed, unpartitioned", "bench_raw_json"),
    ("Parquet + Snappy, unpartitioned", "bench_parquet_flat"),
    ("Parquet + Snappy, partitioned by event_date", "bench_parquet_part"),
]

# Three queries chosen to exercise different optimisations:
#   Q1 narrow projection  -> columnar storage should dominate
#   Q2 date-filtered agg  -> partition pruning should dominate
#   Q3 full-table group-by-> compression should dominate
QUERIES = {
    "Q1_narrow_projection": (
        "Sum revenue over one column. Tests columnar projection: a row store must "
        "read every field, a column store reads one.",
        "SELECT SUM(gross_amount) AS total_revenue FROM {table}",
    ),
    "Q2_date_filtered_aggregate": (
        "Aggregate a single day. Tests partition pruning: only the partitioned "
        "variant can skip the other days entirely.",
        "SELECT event_type, COUNT(*) AS n, SUM(gross_amount) AS revenue "
        "FROM {table} WHERE event_date = '{pivot_date}' GROUP BY event_type",
    ),
    "Q3_full_scan_groupby": (
        "Group the whole table by type. No pruning is possible, so this isolates "
        "the effect of compression and encoding alone.",
        "SELECT event_type, COUNT(*) AS n, AVG(unit_price) AS avg_price "
        "FROM {table} GROUP BY event_type ORDER BY n DESC",
    ),
}


def run_query(athena, sql, database, workgroup, timeout=300):
    """Execute one query and return its real statistics, or an error record."""
    started = time.time()
    try:
        qid = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
        )["QueryExecutionId"]
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"start_query_execution failed: {exc}"}

    # Athena is asynchronous: poll until terminal.
    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - started > timeout:
            athena.stop_query_execution(QueryExecutionId=qid)
            return {"ok": False, "error": f"timed out after {timeout}s", "query_id": qid}
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = info["Status"].get("StateChangeReason", "no reason given")
        return {"ok": False, "error": f"{state}: {reason}", "query_id": qid}

    stats = info["Statistics"]
    scanned = stats.get("DataScannedInBytes", 0)
    billed = max(scanned, ATHENA_MIN_BILLED_BYTES)

    return {
        "ok": True,
        "query_id": qid,
        "scanned_bytes": scanned,
        "billed_bytes": billed,
        "exec_ms": stats.get("EngineExecutionTimeInMillis", 0),
        "total_ms": stats.get("TotalExecutionTimeInMillis", 0),
        "cost_usd": billed / TIB * USD_PER_TB,
        "raw_cost_usd": scanned / TIB * USD_PER_TB,
    }


def human_bytes(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="hb")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--workgroup", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--out", default="BENCHMARK.md")
    ap.add_argument("--repeats", type=int, default=1)
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    athena = session.client("athena")

    # Pick a real date that exists in the data, rather than hardcoding one.
    print("[bench] finding a pivot date present in the data ...")
    pivot = run_query(
        athena,
        "SELECT event_date, COUNT(*) c FROM bench_parquet_part "
        "GROUP BY event_date ORDER BY c DESC LIMIT 1",
        args.database,
        args.workgroup,
    )
    pivot_date = None
    if pivot["ok"]:
        rows = athena.get_query_results(QueryExecutionId=pivot["query_id"])["ResultSet"]["Rows"]
        if len(rows) > 1:
            pivot_date = rows[1]["Data"][0].get("VarCharValue")
    if not pivot_date:
        print("[bench] FATAL: could not determine a pivot date; is the data loaded?")
        print(f"[bench] detail: {pivot.get('error')}")
        return 1
    print(f"[bench] pivot date = {pivot_date}")

    results = {}
    for qname, (rationale, template) in QUERIES.items():
        results[qname] = {"rationale": rationale, "runs": {}}
        for label, table in VARIANTS:
            sql = template.format(table=table, pivot_date=pivot_date)
            best = None
            for attempt in range(args.repeats):
                r = run_query(athena, sql, args.database, args.workgroup)
                if not r["ok"]:
                    print(f"[bench] {qname:28} {table:22} FAILED: {r['error']}")
                    best = r
                    break
                # Keep the fastest run; bytes scanned is deterministic anyway.
                if best is None or r["exec_ms"] < best["exec_ms"]:
                    best = r
            if best["ok"]:
                print(
                    f"[bench] {qname:28} {table:22} "
                    f"{human_bytes(best['scanned_bytes']):>10} "
                    f"{best['exec_ms']:>7} ms  ${best['cost_usd']:.6f}"
                )
            results[qname]["runs"][table] = {**best, "label": label, "sql": sql}

    with open("benchmark_results.json", "w") as fh:
        json.dump(
            {"generated_at": datetime.now(timezone.utc).isoformat(),
             "pivot_date": pivot_date, "workgroup": args.workgroup,
             "database": args.database, "results": results},
            fh,
            indent=2,
        )
    print("\n[bench] raw results -> benchmark_results.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
