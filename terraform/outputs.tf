# hb-ecommerce-lakehouse — outputs
# Owner: Hemanjali Buchireddy

output "data_bucket" {
  description = "Lakehouse bucket holding bronze, quarantine, silver, gold and bench."
  value       = aws_s3_bucket.data.id
}

output "results_bucket" {
  description = "Athena query results bucket."
  value       = aws_s3_bucket.results.id
}

output "firehose_stream_name" {
  description = "Target for the event generator in stream mode."
  value       = aws_kinesis_firehose_delivery_stream.events.name
}

output "glue_database" {
  description = "Glue catalog database queried by Athena."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "state_machine_arn" {
  description = "Batch pipeline. Start with: aws stepfunctions start-execution --state-machine-arn <this>"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "dashboard_workgroup" {
  description = "Athena workgroup for the Vercel dashboard (scan-capped)."
  value       = aws_athena_workgroup.dashboard.name
}

output "benchmark_workgroup" {
  description = "Athena workgroup for Phase 2 (result reuse disabled)."
  value       = aws_athena_workgroup.benchmark.name
}

output "sns_topic_arn" {
  description = "Alert topic. The email subscription stays pending until Hemanjali confirms it."
  value       = aws_sns_topic.alerts.arn
}

# ---------------------------------------------------------------------------
# Dashboard credentials.
#
# Marked sensitive so they never appear in `terraform apply` output or CI logs.
# Read them deliberately with:
#     terraform output -raw dashboard_access_key_id
#     terraform output -raw dashboard_secret_access_key
#
# They are piped straight into `vercel env add` by `make dashboard-env` and are
# never written to .env or committed.
#
# Note: these values DO live in terraform.tfstate in plaintext, which is why
# terraform.tfstate is gitignored. That is inherent to Terraform-managed IAM
# keys, not specific to this project.
# ---------------------------------------------------------------------------

output "dashboard_access_key_id" {
  description = "Access key ID for the read-only dashboard user."
  value       = aws_iam_access_key.dashboard_reader.id
  sensitive   = true
}

output "dashboard_secret_access_key" {
  description = "Secret for the read-only dashboard user. Goes only to Vercel's encrypted env store."
  value       = aws_iam_access_key.dashboard_reader.secret
  sensitive   = true
}

# ---------------------------------------------------------------------------
# Cost transparency
# ---------------------------------------------------------------------------

output "continuously_billing_resources" {
  description = "Everything in this stack that costs money while completely idle. Verified against the AWS Pricing API."
  value = {
    alarm_metric_count      = var.enable_freshness_metric ? 4 : 3
    alarm_cost_usd_month    = format("%.2f", (var.enable_freshness_metric ? 4 : 3) * 0.10)
    custom_metric_count     = var.enable_freshness_metric ? 1 : 0
    custom_metric_usd_month = format("%.2f", (var.enable_freshness_metric ? 1 : 0) * 0.30)
    s3_storage_usd_month    = "~0.06 (about 2.5 GB at 0.023/GB-month)"
    total_idle_usd_month = format(
      "%.2f",
      (var.enable_freshness_metric ? 4 : 3) * 0.10 + (var.enable_freshness_metric ? 1 : 0) * 0.30 + 0.06
    )
    zero_idle_cost = "Firehose, Lambda, Glue, Step Functions and Athena bill only per use"
  }
}
