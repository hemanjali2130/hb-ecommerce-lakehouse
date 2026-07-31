# Cost estimate — no free tier assumed

Project: **hb-ecommerce-lakehouse** · Owner: **Hemanjali Buchireddy**
Account 762233768052 · us-east-1 · priced 2026-07-31

**Every unit price below was pulled from the AWS Pricing API on this machine today.**
None are from memory. The *quantities* are my engineering estimates and are labelled as such.
No free-tier discount is applied anywhere in this document.

---

## 1. Verified unit prices (us-east-1)

| Service | Unit price | Source |
|---|---|---|
| S3 Standard storage | $0.023 / GB-month | Pricing API |
| S3 PUT/COPY/POST/LIST | $0.005 / 1,000 requests | Pricing API |
| S3 GET | $0.0004 / 1,000 requests | Pricing API |
| Athena | **$5.00 / TB scanned**, 10 MB minimum per query | Pricing API |
| Glue ETL (standard) | $0.44 / DPU-hour | Pricing API |
| **Glue Flex ETL** | **$0.29 / DPU-hour** | Pricing API |
| Glue Crawler | $0.44 / DPU-hour, 10-min minimum per crawl | Pricing API |
| Glue Data Catalog | $1.00 / 1M requests (first 1M objects stored free) | Pricing API |
| Data Firehose | $0.080 / GB ingested | Pricing API |
| Lambda requests | $0.0000002 / request ($0.20 per 1M) | Pricing API |
| Lambda duration | $0.0000166667 / GB-second | Pricing API |
| Step Functions Standard | $0.000025 / state transition | Pricing API |
| **CloudWatch alarm** | **$0.10 / alarm / month (standard resolution)** | Pricing API |
| **CloudWatch custom metric** | **$0.30 / metric / month (first 10,000)** | Pricing API |
| CloudWatch Logs Insights | $0.005 / GB scanned | Pricing API |
| CloudWatch Logs storage | $0.03 / GB-month | Pricing API |
| SNS email notification | $2.00 / 100,000 ($0.00002 each) | Pricing API |
| Kinesis Data Streams (provisioned) | $0.015 / shard-hour = **$10.80/mo idle** | Pricing API |
| OpenSearch t3.small.search | $0.036 / hour = **$25.92/mo idle** | Pricing API |

---

## 2. Recommended minimal build

Design choices made specifically to cut cost, each with the reason:

| Choice | Instead of | Saves |
|---|---|---|
| Data Firehose only | Kinesis Data Streams | **$10.80/mo idle** |
| CloudWatch Logs Insights | OpenSearch domain | **$25.92/mo idle** |
| Explicit Glue Catalog tables in Terraform | Glue Crawler | ~$0.15 per crawl, and the tables are destroyable |
| **Glue Flex** for all three ETL jobs | Standard Glue | 34% of Glue compute |
| Benchmark corpus written **bulk to S3** | Streamed through Firehose | $0.16 |
| Reuse the existing "My Monthly Cost Budget" | 4th new budget | avoids per-budget/day charge |
| 2 GB benchmark corpus | 5 GB | ~$0.13 |

### Benchmark compression — decided explicitly

`bench_raw_json` is stored **uncompressed**. This matters and is easy to get wrong: Athena
bills on *bytes actually read from S3*, so a gzipped 2 GB JSON table would report ~300 MB
scanned, not 2 GB — collapsing the measured spread from ~200x to ~30x and making the printed
GB figures disagree with what Athena reports.

Storing it uncompressed makes the benchmark the textbook comparison (raw JSON vs
columnar+compressed) and keeps every number in BENCHMARK.md consistent with
`DataScannedInBytes`. Cost of that choice: ~$0.04/month extra S3 storage.

The bronze landing zone *is* gzipped — that is the production-correct choice and is separate
from the benchmark tables.
| Athena scan cutoff on the dashboard workgroup | unbounded | caps worst case |

### One-time cost to build, test, and benchmark the whole thing

Quantities are estimates; unit prices are verified.

| Line item | Estimated quantity | Cost |
|---|---|---|
| Glue Flex — 3 jobs × 2 DPU × ~9 min, 10 full pipeline runs ⁽ᵃ⁾ | ~9 DPU-hours | **$2.61** |
| Athena — benchmark: 3 queries × 3 variants × 2 runs | ~6.4 GB scanned total | **$0.07** |
| Athena — dev + dashboard queries (~100, partition-pruned) | ~1 GB | **$0.01** |
| Firehose — live demo stream | 0.2 GB | **$0.02** |
| Lambda — validator, ~500 invocations × 3 s × 512 MB | 750 GB-s | **$0.02** |
| Step Functions — 10 runs × ~20 transitions | 200 transitions | **$0.01** |
| S3 requests — PUT/GET across all stages | ~15,000 | **$0.03** |
| CloudWatch Logs — ingest of Glue + Lambda logs | ~0.2 GB | **$0.10** |
| **TOTAL ONE-TIME** | | **≈ $2.87** |

⁽ᵃ⁾ The Glue line is the single largest one-time item and the one most likely to be wrong.
Glue bills per second with a 1-minute minimum, **but the DPU clock includes Spark startup**,
which routinely runs 2–4 minutes on a cold cluster before job code executes. I have budgeted
9 min/job rather than the 5 min a naive estimate gives. **This line will be replaced with
observed DPU-seconds from the first real run**, and COST_ESTIMATE.md updated — estimates do
not stay in this file once measurements exist.

Glue **Flex** ($0.29/DPU-hr) is used for silver and gold. Flex is *not* used for any job that
Step Functions retries on a timeout: Flex jobs can sit queued for minutes, so a timeout retry
could fire while the original is still waiting to start. Those run on standard Glue.

### Recurring cost, per month, if the stack is left standing

| Line item | Quantity | Cost/month |
|---|---|---|
**Counted from `terraform plan`, not estimated** ⁽ᵇ⁾:

| Line item | Quantity | Cost/month |
|---|---|---|
| `hb-validator-error-rate` (metric-math: Errors + Invocations) | **2** alarm-metrics | **$0.20** |
| `hb-state-machine-failed` (AWS/States ExecutionsFailed) | **1** alarm-metric | **$0.10** |
| `hb-data-freshness-lag` | **1** alarm-metric | **$0.10** |
| Custom metric `DataFreshnessLagSeconds` | **1** metric | **$0.30** |
| S3 storage (2 GB corpus + ~0.5 GB curated) | 2.5 GB | **$0.06** |
| CloudWatch Logs storage | 0.2 GB | **$0.01** |
| **TOTAL RECURRING** | | **≈ $0.77 / month** |
| *with `enable_freshness_metric = false`* | | ***≈ $0.37 / month*** |

⁽ᵇ⁾ **This line was wrong in an earlier draft and the reconciliation caught it.** Two errors,
both in the expensive direction:

1. CloudWatch bills **per metric analyzed**, not per alarm. The Lambda error-rate alarm is a
   metric-math alarm over `Errors` and `Invocations`, so it counts as **2** alarm-metrics, not 1.
2. The first draft emitted **three** custom metrics (`DataFreshnessLagSeconds`,
   `GoldFactRowCount`, `PipelineFailed`) at $0.30 each — $0.90/month, triple the $0.30 estimated.

Both were fixed rather than just re-documented:
- `PipelineFailed` was **deleted**. CloudWatch's own `AWS/States ExecutionsFailed` is free and
  fires on exactly the same condition, so the custom metric was paying $0.30/month for a
  duplicate signal. The Step Functions role lost its `cloudwatch:PutMetricData` grant as a
  direct result — a cost decision that also shrank an IAM policy.
- `GoldFactRowCount` was **deleted**. The dashboard already gets row counts from Athena.

Net effect: idle cost went from **$1.46/month** as originally written to **$0.77/month**.

### After `terraform destroy`

**$0.00 / month.** Nothing survives. There are no reserved instances, no provisioned
capacity, and no per-hour resources in this design.

---

## 3. The only things that bill continuously

Per the standing instruction to warn loudly before creating anything that bills whether or
not it is used, the complete list for this design:

| Resource | Cost when completely idle | Can it be dropped? |
|---|---|---|
| 3 CloudWatch alarms | $0.30/mo | Yes, but the spec requires them |
| 1 CloudWatch custom metric (freshness) | $0.30/mo | **Yes** — the dashboard already computes freshness from Athena. The metric exists only so CloudWatch can *alarm* on it. Drop it and lose the freshness alarm. |
| S3 storage | $0.06/mo | Shrink the corpus, or destroy after benchmarking |

**Nothing else has an idle charge.** Firehose, Lambda, Glue, Step Functions, and Athena are
strictly pay-per-use and cost exactly $0.00 when nothing is running.

**Not being created:** Kinesis Data Streams ($10.80/mo idle), OpenSearch ($25.92/mo idle),
QuickSight (per-user monthly subscription), EMR, NAT Gateway, RDS.

---

## 4. Guards, in order of how fast they act

1. **Athena workgroup per-query bytes-scanned cutoff** — instant, hard. A query exceeding the
   limit is killed by Athena before it scans. This is the guard that stops a recruiter
   refreshing the dashboard from running up a bill.
2. **Generator off by default** — no data flows unless started by hand.
3. **`make demo-down`** — tears down everything billable in one command.
4. **Server-side 60 s cache** on every dashboard Athena call.
5. **AWS Budget alert** — a backstop, not a guard. Budgets evaluate *actual* spend and lag by
   several hours. Do not rely on it to stop anything.

---

## 5. Worst realistic overrun

If the generator were accidentally left running for 24 hours at the configured default rate
(~500 events/sec, ~400 bytes/event): ~17 GB through Firehose = **$1.38**, plus ~$0.40 S3
storage for that month. Roughly **$2** — not catastrophic, and `make demo-down` stops it.

The genuinely dangerous failure mode in a lakehouse is not ingestion, it is **an unbounded
Athena scan**. A single careless `SELECT *` against a 2 GB unpartitioned raw-JSON table costs
$0.01; against a 10 TB table it costs $50. The workgroup cutoff is what makes that
impossible here, and it is configured before the dashboard ever runs a query.
