# Phase 0 — Preflight Report

Project: **hb-ecommerce-lakehouse** · Owner: **Hemanjali Buchireddy**
Deployment account **904233128322** · us-east-1 · probed 2026-07-31

Everything below was probed with real CLI calls before any application code was
written or any billable resource created. The architecture in ARCHITECTURE.md
follows directly from these findings.

## Account posture

| Check | Result |
|---|---|
| Account type | Standard AWS account (not Academy/Learner Lab — no session token, full IAM access, standalone) |
| Organizations | Not a member of any organization |
| Root user | MFA **enabled**, **no root access keys** |
| Build identity | IAM user `hb-builder` on a scoped policy (see IAM_DESIGN.md) — not AdministratorAccess |
| Month-to-date spend at start | **$0.00095** (S3 only) |
| Existing budgets | none — this project created the first ($5/month, before any billable resource) |
| Pre-existing resources | one unrelated S3 bucket and IAM role from a separate Snowflake project — untouched by this build |

## Service availability (one read-only call each)

All services required by the design answered **ALLOWED** under the build
credentials: S3, Glue, Athena, Lambda, Step Functions, Kinesis, Data Firehose,
CloudWatch, CloudWatch Logs, OpenSearch, EMR, EMR Serverless, SNS, IAM, Budgets,
Cost Explorer.

Re-probing after the build user was downgraded from bootstrap admin to its
scoped policy showed denials on **exactly three services: Kinesis Data Streams,
OpenSearch, and EMR** — precisely the three the architecture rejected on cost
grounds. The permission boundary and the cost design agree: an accidental
`create-cluster` fails at the IAM layer, not at the invoice.

## Local toolchain

| Tool | State at preflight |
|---|---|
| AWS CLI | 2.28.21, authenticated |
| Node.js / npm | v25.4.0 / 11.7.0 |
| Python | 3.13.4 |
| Vercel CLI | 54.4.1, authenticated |
| Terraform | not installed → installed **1.15.8** from `hashicorp/tap` (the formula left homebrew-core with the BSL license change) |
| Docker | **not installed** — which is why the validator Lambda is a pure-stdlib zip, no container image, no compiled wheels |

## Pricing probe (AWS Pricing API, us-east-1, 2026-07-31)

The decisions that shaped the architecture, priced before adoption:

| Component | Unit price | Idle cost/month | Decision |
|---|---|---|---|
| Data Firehose | $0.080/GB | **$0** | ✅ ingest path |
| Kinesis Data Streams (provisioned) | $0.015/shard-hr | **$10.80** | ❌ rejected — 2× the entire budget, idle |
| Kinesis Data Streams (on-demand) | $0.040/stream-hr | **$28.80** | ❌ rejected |
| OpenSearch `t3.small.search` | $0.036/hr | **$25.92** | ❌ rejected — CloudWatch Logs Insights instead |
| QuickSight | per-user subscription | recurring | ❌ rejected — custom Next.js dashboard instead |
| Glue Flex ETL | $0.29/DPU-hr | $0 | ✅ all three jobs |
| Glue Crawler | $0.44/DPU-hr, 10-min minimum | $0 | ❌ rejected — explicit catalog tables in Terraform |
| Athena | $5.00/TB scanned, 10 MB min/query | $0 | ✅ query layer, with workgroup scan caps |
| CloudWatch alarm / custom metric | $0.10 / $0.30 per month | $0.10–0.30 | ✅ the only standing costs accepted |

Resulting idle cost target: **$0.77/month** (4 alarm-metrics + 1 custom metric
+ S3 storage). Verified after the build in COST_ESTIMATE.md.

## Benchmark design decision (made here, because it changes the Terraform)

The three Phase 2 benchmark variants (raw JSON, flat Parquet, partitioned
Parquet) are first-class Terraform outputs built by one Glue job from one silver
read, so the comparison is provably like-for-like. `bench_raw_json` is stored
**uncompressed** because Athena bills bytes actually read from S3 — gzip would
have made every measured number disagree with `DataScannedInBytes`. Measurement
runs in a dedicated workgroup with **result reuse disabled**, since a reused
result reports zero bytes scanned and would silently invalidate the table.
