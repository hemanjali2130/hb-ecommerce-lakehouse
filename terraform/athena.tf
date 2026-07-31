# hb-ecommerce-lakehouse — Athena workgroups
# Owner: Hemanjali Buchireddy
#
# TWO workgroups, deliberately, because the dashboard and the benchmark need
# opposite settings:
#
#   hb-dashboard-wg  — result reuse ON, tight scan cutoff. Optimised for cost.
#   hb-benchmark-wg  — result reuse OFF, loose cutoff. Optimised for honest
#                      measurement.
#
# Result reuse is the subtle one. A reused Athena result reports
# DataScannedInBytes = 0 and EngineExecutionTimeInMillis near zero. Running the
# Phase 2 benchmark in a reuse-enabled workgroup would silently produce a table
# of zeros that looks like a spectacular optimisation result and is entirely
# meaningless. Separating the workgroups makes that mistake impossible.

# ---------------------------------------------------------------------------
# Dashboard workgroup — every query the Vercel app runs goes through here
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "dashboard" {
  name        = "${var.project}-dashboard-wg"
  description = "Vercel dashboard queries. Scan-capped and reuse-enabled to bound cost."
  state       = "ENABLED"

  # Deletes the workgroup even if it has query history.
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # THE hard cost guard. Athena refuses to run a query whose estimated scan
    # exceeds this, so no amount of dashboard refreshing can produce a large bill.
    bytes_scanned_cutoff_per_query = var.dashboard_scan_cutoff_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.results.id}/dashboard/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags       = merge(var.common_tags, { Name = "${var.project}-dashboard-wg" })
  depends_on = [aws_budgets_budget.ceiling]
}

# ---------------------------------------------------------------------------
# Benchmark workgroup — Phase 2 measurements only
# ---------------------------------------------------------------------------

resource "aws_athena_workgroup" "benchmark" {
  name        = "${var.project}-benchmark-wg"
  description = "Phase 2 benchmark runs. Result reuse DISABLED so every execution reports real bytes scanned."
  state       = "ENABLED"

  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Loose enough for the raw-JSON variant to scan the whole corpus, which is
    # the entire point of the comparison.
    bytes_scanned_cutoff_per_query = var.benchmark_scan_cutoff_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.results.id}/benchmark/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags       = merge(var.common_tags, { Name = "${var.project}-benchmark-wg" })
  depends_on = [aws_budgets_budget.ceiling]
}
