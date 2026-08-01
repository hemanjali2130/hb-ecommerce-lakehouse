# Benchmark — measured, not estimated

**hb-ecommerce-lakehouse** — Hemanjali Buchireddy
AWS account 904233128322 · us-east-1 · Athena engine v3 · measured 2026-07-31

Every number below was read from Athena's own `GetQueryExecution` response:
`Statistics.DataScannedInBytes` and `Statistics.EngineExecutionTimeInMillis`.
Nothing here is estimated, interpolated, or filled in. **All 9 of 9 queries
succeeded on the final run.** One cell failed on the first run; the failure, its
root cause and the fix are documented under Q2 rather than quietly removed.

Raw output: `benchmark_results.json`. Harness: `scripts/run_benchmark.py`.

---

## Method

Three analytical queries against **three physical layouts of the same
6,024,034 rows**. All three tables are written by one Glue job
(`glue/bench_job.py`) from a single cached DataFrame in a single run, so they are
provably identical logically — the only differences are physical.

| Table | Layout | On-disk |
|---|---|---|
| `bench_raw_json` | JSON, **uncompressed**, unpartitioned | 1.47 GiB |
| `bench_parquet_flat` | Parquet + Snappy, unpartitioned | ~30 MB |
| `bench_parquet_part` | Parquet + Snappy, partitioned by `event_date` (4 partitions) | ~30 MB |

**Result reuse is disabled at the workgroup level** (`hb-benchmark-wg`). This is
not optional: a reused Athena result reports `DataScannedInBytes = 0`, which
would have produced a table of zeros that looks like a spectacular optimisation
and means nothing.

`bench_raw_json` is stored **uncompressed on purpose**. Athena bills bytes
actually read from S3, so gzipped JSON would report roughly one seventh of its
logical size — collapsing the measured spread and making every GB figure here
disagree with what Athena reports.

**Cost basis:** $5.00 per TB scanned (verified from the AWS Pricing API), with
Athena's **10 MB minimum per query** applied. Both raw and billed bytes are shown
because quoting only raw bytes would overstate the saving.

---

## Results

### Q1 — narrow projection (columnar effect)
```sql
SELECT SUM(gross_amount) AS total_revenue FROM <table>
```
A row store must read every field; a column store reads one.

| Layout | Scanned | Billed | Time | Cost | vs raw JSON |
|---|---:|---:|---:|---:|---:|
| raw JSON | **1,575,191,600 B** (1.47 GiB) | 1.47 GiB | 1,847 ms | **$0.007163** | — |
| Parquet flat | **6,342,809 B** (6.05 MB) | 10 MB | 977 ms | $0.000048 | **248× less scanned** |
| Parquet partitioned | **6,329,256 B** (6.04 MB) | 10 MB | 1,331 ms | $0.000048 | **249× less scanned** |

### Q2 — date-filtered aggregate (partition-pruning effect)
```sql
SELECT event_type, COUNT(*), SUM(gross_amount)
FROM <table> WHERE event_date = '2026-07-31' GROUP BY event_type
```

| Layout | Scanned | Billed | Time | Cost | vs raw JSON |
|---|---:|---:|---:|---:|---:|
| raw JSON | **1,575,191,600 B** (1.47 GiB) | 1.47 GiB | 1,445 ms | **$0.007163** | — |
| Parquet flat | **7,666,730 B** (7.31 MB) | 10 MB | 831 ms | $0.000048 | **206× less scanned** |
| Parquet partitioned | **7,597,071 B** (7.24 MB) | 10 MB | 809 ms | $0.000048 | **207× less scanned** |

> **This cell failed on the first measurement run and was fixed.** The original
> failure is kept on record because it is informative:
> ```
> HIVE_BAD_DATA: Malformed Parquet file. Field event_date's type INT32 in
> parquet file s3://.../bench/parquet_flat/part-00000-....snappy.parquet is
> incompatible with type varchar defined in table schema
> ```
> **Root cause.** Silver is written with `partitionBy("event_date")`. When Spark
> reads a partitioned dataset back it *infers* the partition column's type,
> turning what was a string into `DateType`. In the partitioned variant this is
> invisible — Athena reads partition values from the S3 path as strings. But in
> the unpartitioned variants `event_date` becomes a real column in the file,
> stored as INT32 (date), which does not match the `string` column declared in
> the Glue table. Only Q2 references `event_date`, which is why Q1 and Q3
> succeeded against the same table on the same run.
>
> **Fix:** `bench_job.py` now casts `event_date` to string before writing, so the
> physical type is identical across all three variants — which is what a
> like-for-like benchmark requires. The number above is from the re-run after the
> fix; **all 9 of 9 queries now succeed.**

### Q3 — full-table group-by (compression effect, no pruning possible)
```sql
SELECT event_type, COUNT(*), AVG(unit_price)
FROM <table> GROUP BY event_type ORDER BY 2 DESC
```

| Layout | Scanned | Billed | Time | Cost | vs raw JSON |
|---|---:|---:|---:|---:|---:|
| raw JSON | **1,575,191,600 B** (1.47 GiB) | 1.47 GiB | 1,664 ms | **$0.007163** | — |
| Parquet flat | **8,572,836 B** (8.18 MB) | 10 MB | 830 ms | $0.000048 | **184× less scanned** |
| Parquet partitioned | **8,278,577 B** (7.89 MB) | 10 MB | 1,338 ms | $0.000048 | **190× less scanned** |

---

## What the numbers actually say

**1. Columnar format plus compression does essentially all the work here:
184×–249× less data scanned, and a 149× cost reduction per query
($0.007163 → $0.000048).** Q1 is the clearest case: summing one column reads
6 MB of Parquet against 1.47 GiB of JSON, because a row-oriented format cannot
skip the fields you did not ask for.

**2. Partition pruning added almost nothing at this scale — and that is an
honest, useful finding rather than a disappointing one.** Q2 filters to a single
day out of four. Now that the flat variant works, the two can be compared
directly on the same query: **7.31 MB unpartitioned vs 7.24 MB partitioned —
a 0.9% difference.** Comparing Q3 (no filter) with Q2 on the partitioned table
gives the same story: 7.89 MB vs 7.24 MB, ~8%.

The reason is that the *entire* Parquet dataset is ~30 MB across 4 partitions.
Pruning to one partition saves a few MB — and Athena's 10 MB per-query minimum
swallows the difference entirely, so **both cost exactly the same**. Partitioning
pays off when a partition is large enough that skipping it saves more than the
billing floor: at ~10 GB/day with 90 days retained, pruning to one day is a 90×
saving. At 30 MB total it is noise.

Partitioning is still the right design choice — it is what makes the table scale
past the point where it would matter, and the cost of adding it later (rewriting
history) is far higher than adding it now. But claiming a partition-pruning win
from this dataset would be dishonest, and the measurement says so.

**3. Execution time does not track bytes scanned.** Raw JSON scans 249× more
than Parquet on Q1 but takes only 1.9× longer (1,847 ms vs 977 ms). Run-to-run
variance is also large — the same raw-JSON query measured 1,379 ms, 2,274 ms and
1,847 ms across runs while bytes scanned stayed **byte-identical** at
1,575,191,600 every time. Athena
parallelises the scan across many workers, so wall-clock is dominated by
per-query overhead and planning, not I/O, at this size. **Cost tracks bytes;
latency does not.** Anyone quoting a speed-up as the headline benefit of Parquet
at small scale is quoting the wrong metric — the money is in bytes scanned.

**4. The 10 MB minimum dominates small queries.** Every Parquet query here
scanned 6–8 MB and was billed for 10 MB. Optimising below the floor is wasted
effort; the first real win is getting *above* it and then reducing.

---

## Cost of running this benchmark

| | |
|---|---|
| Queries executed | **9 of 9 succeeded** |
| Total billed bytes | 4.46 GiB |
| **Total cost** | **$0.0218** |

At production scale the same three-query set against raw JSON at 100 GB/day
would cost roughly **$0.49 per run**; against partitioned Parquet, about
**$0.0001**.

---

## Reproducing

```bash
make run-pipeline-bench     # rebuild the three variants from one silver read
make benchmark              # re-measure; writes benchmark_results.json
```

Bytes scanned are deterministic and will reproduce exactly. Execution times will
not — they varied by up to 60% between runs here for identical byte counts,
which is the reason every conclusion above is drawn from bytes rather than
milliseconds.
