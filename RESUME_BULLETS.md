# Resume bullets

**Hemanjali Buchireddy** — hb-ecommerce-lakehouse

Pattern: action → mechanism → measured outcome. Every figure below was observed
during the build and is traceable to a file in this repo. No estimates, no
projections, and the word "leveraged" appears nowhere.

---

**1.**
Built a serverless e-commerce lakehouse on AWS — Data Firehose ingestion, a
schema-validating Lambda transform, Glue PySpark medallion jobs and a Kimball
star schema queried through Athena, all provisioned by Terraform — then
benchmarked three physical layouts of the same 6,024,034 rows and reduced bytes
scanned **248×** (1.47 GiB → 6.05 MB) and cost per analytical query **149×**
($0.007163 → $0.000048).

**2.**
Cut standing infrastructure cost to **$0.77/month** against a $5 ceiling by
pricing every component from the AWS Pricing API before adopting it and
replacing Kinesis Data Streams ($10.80/month idle) and OpenSearch
($25.92/month idle) with Data Firehose ($0 idle) and CloudWatch Logs Insights;
the complete build, including 8 Glue job runs and 9 benchmark queries, measured
**$0.21**.

**3.**
Implemented in-flight schema validation as a Firehose transform that writes every
rejected record to a `reject_reason`-partitioned quarantine prefix *before*
dropping it from the stream — and returns `ProcessingFailed` if that write fails,
so no record is discarded without being persisted — quarantining **107,966 of
6,132,000** generated records (**1.76%**) across **6 distinct rejection reasons**
with **zero** quarantine write failures.

---

## Where each number comes from

| Figure | Source |
|---|---|
| 6,024,034 rows | `hb-silver-job` stdout, `[silver] rows after dedupe` |
| 1.47 GiB → 6.05 MB, 248× | `BENCHMARK.md` Q1 — Athena `DataScannedInBytes` |
| $0.007163 → $0.000048, 149× | `BENCHMARK.md` Q1 — billed bytes at $5/TB, 10 MB minimum applied |
| $0.77/month idle | `COST_ESTIMATE.md` — 4 alarm-metrics + 1 custom metric + S3, counted from `terraform plan` |
| $10.80 / $25.92 per month idle | AWS Pricing API: KDS $0.015/shard-hour, OpenSearch `t3.small.search` $0.036/hour |
| $0.21 total build | 2,224 measured Glue DPU-seconds at $0.29/DPU-hr + 4.72 GiB Athena + Firehose |
| 107,966 / 6,132,000 / 1.76% | generator output (107,765 bulk) + validator CloudWatch logs (201 streamed) |
| 6 rejection reasons | `quarantine/reject_reason=*` prefixes in S3 |
| zero write failures | validator log field `quarantine_write_failures: 0` on every invocation |

## Deliberately not claimed

- **No speed-up figure.** Raw JSON scans 248× more than Parquet but runs only
  1.8× slower (1,379 ms vs 780 ms) — Athena parallelises the scan, so wall-clock
  is dominated by planning overhead at this size. The saving is in bytes and
  therefore cost, not latency, and quoting a latency number would misrepresent it.
- **No partition-pruning win.** Measured at 7.89 MB unfiltered vs 7.24 MB
  filtered to one day — ~8%, and Athena's 10 MB per-query minimum makes both cost
  the same. Partitioning is the correct design for scale, but this dataset is too
  small for it to pay, and `BENCHMARK.md` says so.
- **No uptime, SLA or "production traffic" claim.** This is a portfolio project
  running synthetic data on demand.
