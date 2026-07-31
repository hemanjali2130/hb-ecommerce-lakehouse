# hb-ecommerce-lakehouse — input variables
# Owner: Hemanjali Buchireddy

variable "project" {
  description = "Short project slug. Prefixes every resource name and every IAM ARN pattern in hb-builder-policy.json."
  type        = string
  default     = "hb"
}

variable "aws_region" {
  description = "Deployment region. us-east-1 chosen for lowest Athena/S3 pricing and full service availability."
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "Hemanjali's AWS account. Pinned so a mis-set CLI profile fails loudly instead of building in the wrong account."
  type        = string
  default     = "904233128322"
}

variable "aws_profile" {
  description = "Named CLI profile holding the hb-builder credentials."
  type        = string
  default     = "hb"
}

variable "bucket_prefix" {
  description = "Globally unique S3 name prefix. Must match the hb-ecom-lakehouse-* pattern allowed by hb-builder-policy.json."
  type        = string
  default     = "hb-ecom-lakehouse-904233"
}

variable "alert_email" {
  description = "Destination for budget alerts, CloudWatch alarms, and the Step Functions failure branch."
  type        = string
  default     = "hemanjalibreddy@gmail.com"
}

variable "budget_ceiling_usd" {
  description = "Hard monthly budget ceiling in USD."
  type        = number
  default     = 5
}

# ---------------------------------------------------------------------------
# Cost guards
# ---------------------------------------------------------------------------

variable "dashboard_scan_cutoff_bytes" {
  description = <<-EOT
    Per-query bytes-scanned cutoff on the dashboard Athena workgroup. Athena kills any
    query exceeding this BEFORE it scans, so a recruiter hammering refresh physically
    cannot run up the bill. 200 MB at $5/TB caps a single query at ~$0.001.
  EOT
  type        = number
  default     = 209715200 # 200 MiB
}

variable "benchmark_scan_cutoff_bytes" {
  description = "Cutoff for the benchmark workgroup. Higher, because the raw-JSON variant is meant to scan the full corpus."
  type        = number
  default     = 10737418240 # 10 GiB
}

variable "glue_worker_count" {
  description = "G.1X workers per Glue job. 2 is the minimum Glue accepts; each worker is 1 DPU."
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention. Short by design — log storage bills $0.03/GB-month indefinitely otherwise."
  type        = number
  default     = 7
}

# ---------------------------------------------------------------------------
# Optional / cost-toggleable components
# ---------------------------------------------------------------------------

variable "enable_freshness_metric" {
  description = <<-EOT
    Whether to emit the custom data-freshness metric and its CloudWatch alarm.
    Costs $0.30/month for the metric plus $0.10/month for the alarm, billed whether
    or not the pipeline runs. The dashboard computes freshness from Athena either
    way; this exists solely so CloudWatch can alarm on it.
  EOT
  type        = bool
  default     = true
}

variable "common_tags" {
  description = "Applied to every taggable resource so cost allocation and cleanup can filter on them."
  type        = map(string)
  default = {
    Owner      = "Hemanjali Buchireddy"
    Project    = "hb-ecommerce-lakehouse"
    ManagedBy  = "terraform"
    CostCenter = "portfolio"
  }
}
