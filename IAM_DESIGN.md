# IAM design

**hb-ecommerce-lakehouse** — Hemanjali Buchireddy
AWS account 904233128322

Every role here was **actually created** and is live in the account; nothing in
this document is hypothetical. The design principle throughout is that **no
component can do another component's job**, so a bug or compromise in one stage
cannot corrupt the others.

---

## Principals at a glance

| Principal | Type | Purpose | Blast radius if compromised |
|---|---|---|---|
| `hb-builder` | IAM user | Runs `terraform apply` from the build machine | `hb-*` resources in one account, $5 ceiling |
| `hb-firehose-role` | Role | Firehose delivery | Write to `bronze/` and `firehose-errors/` only |
| `hb-lambda-validator-role` | Role | Firehose transform | Write to `quarantine/` only |
| `hb-glue-job-role` | Role | Three PySpark jobs | Read bronze/silver, write silver/gold/bench |
| `hb-stepfunctions-role` | Role | Orchestration | Start 3 named Glue jobs, publish to 1 SNS topic. **No S3 at all** |
| `hb-dashboard-reader` | IAM user | Vercel dashboard | Read `gold/` + `bench/`, query one workgroup |

---

## 1. `hb-builder` — the build identity

Attached policy: `hb-builder-policy` (customer-managed, currently **v5**).

Structured as one broad **read-only discovery** statement on `*`, plus write
statements each scoped to `hb-*` resource ARNs.

### The 6,144-byte wall, and what it forced

The first version enumerated every action verb explicitly — `s3:CreateBucket`,
`s3:PutLifecycleConfiguration`, `s3:PutEncryptionConfiguration` and so on. That
version **exceeded AWS's hard 6,144-byte limit on managed policy documents** and
was rejected outright:

```
LimitExceeded: Cannot exceed quota for PolicySize: 6144
```

(IAM ignores whitespace when measuring, so reformatting does not help.)

The policy was restructured to scope by **resource ARN** rather than by action
verb: `"Action": "s3:*"` on `arn:aws:s3:::hb-ecom-lakehouse-*` instead of
twenty individual S3 verbs. Final size: **3,745 bytes**.

This is a genuine trade-off and worth stating plainly. Action-level scoping is
stricter in principle. But the blast radius is defined by *what a principal can
touch*, not *which verbs it may use* — and `s3:*` on `hb-ecom-lakehouse-*` still
cannot read, write or delete a single byte of the pre-existing
`hemanjali-snowflake-retail-904233128322` bucket that shares the account. **IAM
remains action-scoped**, because that is the service where verb-level
distinctions actually change the security outcome.

### What it deliberately cannot do

Verified empirically by running the same probe under `AdministratorAccess` and
then under the scoped policy, and diffing:

| Service | Result | Why that is correct |
|---|---|---|
| Kinesis Data Streams | **DENIED** | Not used — $10.80/month idle |
| OpenSearch | **DENIED** | Not used — $25.92/month idle |
| EMR / EMR Serverless | **DENIED** | Not used — Glue instead |

The scoped policy denies precisely the three expensive services the architecture
rejected on cost grounds. The permissions boundary and the cost design agree,
which is a useful property: an accidental `aws emr create-cluster` fails at the
IAM layer, not at the invoice.

### Known weakness: self-escalation, kept on purpose

The policy grants `iam:CreatePolicyVersion` on `policy/hb-*`, which means
`hb-builder` **can rewrite its own permissions**. That is a textbook privilege
escalation path and an interviewer would be right to flag it.

It was kept deliberately, because `hb-builder` cannot manage its own access keys
(`iam:CreateAccessKey` is scoped to `user/hb-dashboard-reader` only), so a
missing permission would otherwise mean a hard lockout requiring root console
access to fix.

**It earned its keep on the first real use.** `terraform import` of the budget
failed with:

```
AccessDeniedException: not authorized to perform: budgets:ListTagsForResource
```

and three further gaps surfaced during `terraform apply` —
`logs:ListTagsForResource`, `s3:GetAccelerateConfiguration` and
`states:ValidateStateMachineDefinition`. All are **provider read-back calls**:
the Terraform AWS provider re-reads every attribute of a resource after creating
it, and several of those action names are not matched by the obvious wildcard
(`s3:GetBucket*` does not cover `s3:GetAccelerateConfiguration`; S3 uses both
`PutBucketX` and `PutXConfiguration` forms inconsistently). Each was fixed by
publishing a new policy version without needing root.

**The production alternative**, which is what should be said in an interview: the
build identity should be a **role assumed by CI** with a short-lived session and
a **permissions boundary** capping its maximum privilege, not a long-lived user
that can edit its own policy. The boundary is what makes `iam:CreatePolicyVersion`
safe — the role can rewrite its policy but cannot exceed the boundary.

---

## 2. `hb-firehose-role`

**Trusts:** `firehose.amazonaws.com`, with an `sts:ExternalId` condition pinned
to the account ID. That condition addresses the **confused-deputy problem**:
without it, another AWS customer's Firehose could in principle be configured to
assume this role.

| Grant | Scope |
|---|---|
| `s3:PutObject`, `AbortMultipartUpload`, … | `bronze/*` and `firehose-errors/*` **only** |
| `s3:ListBucket` | data bucket, **conditioned** on `s3:prefix` matching those two prefixes |
| `lambda:InvokeFunction` | the validator function ARN **only** — not `lambda:*` on `*` |
| `logs:PutLogEvents` | its own log group |

It cannot write to `silver/`, `gold/`, `bench/` or `quarantine/`.

---

## 3. `hb-lambda-validator-role`

| Grant | Scope |
|---|---|
| `s3:PutObject` | `quarantine/*` **and nothing else** |
| `logs:CreateLogStream`, `logs:PutLogEvents` | its own log group |

**The validator cannot write to `bronze/`.** This is intentional and is the
single tightest grant in the system. Valid records are returned to Firehose,
which writes them under its own role; the Lambda's only write path is the
quarantine prefix. A bug in the validator therefore cannot corrupt the raw
landing zone — the worst it can do is over-quarantine, which is visible,
recoverable and loudly reported on the dashboard.

---

## 4. `hb-glue-job-role`

| Grant | Scope |
|---|---|
| `s3:GetObject` | `bronze/*`, `silver/*` |
| `s3:PutObject`, `DeleteObject` | `silver/*`, `gold/*`, `bench/*` |
| `s3:GetObject` | artifacts bucket (job scripts) |
| Glue Catalog read + partition write | database `hb_lakehouse` and its tables only |
| `cloudwatch:PutMetricData` | **conditioned** on `cloudwatch:namespace = hb/lakehouse` |
| `logs:*` | `/aws-glue/*` |

**Quarantine is deliberately absent.** Quarantine is an audit trail of what the
platform rejected; a batch job that could rewrite it could erase evidence of its
own upstream data-quality failures.

The `PutMetricData` condition matters more than it looks: that action does not
support resource-level permissions, so `"Resource": "*"` is unavoidable. The
namespace condition is what prevents a bug from writing into `AWS/Lambda` or any
other namespace.

---

## 5. `hb-stepfunctions-role`

| Grant | Scope |
|---|---|
| `glue:StartJobRun`, `GetJobRun`, `BatchStopJobRun` | the **three named job ARNs** |
| `sns:Publish` | the one project topic |
| `logs:CreateLogDelivery`, … | `*` — see below |

**No S3 access whatsoever.** The orchestrator starts jobs and reports failures;
it never touches data. If it were compromised it could re-run the pipeline or
send emails, but could not read or exfiltrate a single record.

An earlier revision also granted `cloudwatch:PutMetricData` so the failure branch
could emit a custom `PipelineFailed` metric. That metric was removed for cost
(a custom metric is $0.30/month standing, and `AWS/States ExecutionsFailed` is
free and equivalent) — **and the IAM grant went with it**. A cost decision made
the policy smaller.

The `logs:*LogDelivery` actions are on `"*"` because AWS does not support
resource-level permissions for them; Step Functions logging cannot be configured
without it. Called out rather than hidden.

---

## 6. `hb-dashboard-reader` — the only credential that leaves the machine

This is the identity whose access key is stored in Vercel's encrypted
environment store.

| Grant | Scope |
|---|---|
| `athena:StartQueryExecution`, `GetQueryExecution`, `GetQueryResults` | `hb-dashboard-wg` **only** |
| Glue Catalog metadata | `hb_lakehouse` and its tables |
| `s3:GetObject` | `gold/*` and `bench/*` **only** |
| `s3:ListBucket` | conditioned on `s3:prefix` matching gold/bench |
| `s3:GetObject`, `PutObject` | `results/dashboard/*` only |
| `states:ListExecutions`, `DescribeExecution` | the pipeline state machine |
| `cloudwatch:GetMetricData` | `*` (no resource-level support) |

What it explicitly **cannot** do:

- Read `bronze/`, `silver/` or `quarantine/` object data. The dashboard shows
  quarantine *counts* via Athena over the catalog, never raw rejected payloads —
  which may contain unvalidated customer identifiers.
- Use `hb-benchmark-wg`, whose 10 GB cutoff would permit an expensive scan. It is
  confined to the 200 MB workgroup, so **the worst query it can possibly run
  costs about $0.001**.
- Write to any data prefix, start any Glue job, or read any other bucket.

The scan cutoff is enforced at the workgroup, not in application code, so it
holds even if the dashboard has a bug or its credentials leak.

---

## Threat model summary

| If this leaked | Attacker gets | Attacker cannot |
|---|---|---|
| `hb-dashboard-reader` key | Read-only gold/bench, ~$0.001/query, capped | Touch raw data, write anything, exceed the workgroup cutoff |
| `hb-builder` key | Full control of `hb-*` resources in one account | Touch the pre-existing snowflake bucket, create EMR/KDS/OpenSearch, exceed $5 before the budget alerts |
| A Glue job role | Read/write curated layers | Alter quarantine, invoke Lambda, publish to SNS |
| The validator role | Write quarantine objects | Corrupt bronze, silver or gold |

## Outstanding items

1. **Rotate `hb-builder`'s access key.** Its secret entered a working session
   transcript during setup. Scheduled for after project completion at the
   owner's direction. Rotation requires root console access by design, since
   `hb-builder` cannot manage its own keys.
2. **Add a permissions boundary** to `hb-builder`, which would make the retained
   `iam:CreatePolicyVersion` grant safe rather than merely pragmatic.
3. **Move the build identity to an assumed role** with short-lived credentials
   instead of a long-lived user.
