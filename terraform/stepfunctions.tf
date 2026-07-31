# hb-ecommerce-lakehouse — batch orchestration
# Owner: Hemanjali Buchireddy
#
# Step Functions Standard, not Express: Standard gives per-execution history for
# up to 90 days (which the dashboard reads to show last-run status and duration)
# and bills per state transition — ~20 transitions per run at $0.000025 each is
# $0.0005. Express bills per GB-second and would need CloudWatch Logs to
# reconstruct run history. See ARCHITECTURE.md ADR-004 for Step Functions vs Airflow.
#
# Retry policy: 3 attempts, 30s initial interval, 2.0 backoff — so 30s, 60s, 120s.
# There is deliberately NO TimeoutSeconds on the Glue tasks. The jobs run as Flex
# and can queue before starting; a state-machine timeout would fire while the run
# was still queued and the retry would double-execute. The timeout lives on the
# Glue job itself, whose clock excludes queue time.

resource "aws_sns_topic" "alerts" {
  name = "${var.project}-lakehouse-alerts"
  tags = var.common_tags
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
  # Note: this subscription stays "PendingConfirmation" until Hemanjali clicks
  # the link in the confirmation email. Terraform cannot confirm it.
}

resource "aws_cloudwatch_log_group" "states" {
  name              = "/aws/vendedlogs/states/${var.project}-pipeline"
  retention_in_days = var.log_retention_days
  tags              = var.common_tags
}

locals {
  # Shared retry block. Glue throws ConcurrentRunsExceededException under load,
  # which is transient and exactly what exponential backoff is for.
  glue_retry = [
    {
      ErrorEquals = [
        "Glue.ConcurrentRunsExceededException",
        "Glue.InternalServiceException",
        "Glue.ResourceNumberLimitExceededException",
        "States.TaskFailed",
      ]
      IntervalSeconds = 30
      MaxAttempts     = 3
      BackoffRate     = 2.0
    }
  ]

  pipeline_definition = jsonencode({
    Comment = "hb-ecommerce-lakehouse batch pipeline: bronze -> silver -> gold -> benchmark tables"
    StartAt = "BuildSilver"

    States = {
      # -------------------------------------------------------------------
      BuildSilver = {
        Type     = "Task"
        Comment  = "Dedupe on business key, enforce schema, write Snappy Parquet partitioned by event_date."
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver.name
        }
        Retry      = local.glue_retry
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        ResultPath = "$.silver"
        Next       = "BuildGold"
      }

      # -------------------------------------------------------------------
      BuildGold = {
        Type     = "Task"
        Comment  = "Build the Kimball star schema and emit the data-freshness metric."
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.gold.name
        }
        Retry      = local.glue_retry
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        ResultPath = "$.gold"
        Next       = "ShouldBuildBenchmark"
      }

      # -------------------------------------------------------------------
      # The benchmark tables are only needed when re-measuring for Phase 2.
      # Rebuilding ~2 GB of raw JSON on every routine run would waste Glue DPU
      # hours, so it is opt-in via input: {"build_benchmark": true}.
      ShouldBuildBenchmark = {
        Type = "Choice"
        Choices = [
          {
            Variable      = "$.build_benchmark"
            BooleanEquals = true
            Next          = "BuildBenchmarkTables"
          }
        ]
        Default = "PipelineSucceeded"
      }

      BuildBenchmarkTables = {
        Type     = "Task"
        Comment  = "Materialize the same silver rows as raw JSON, flat Parquet and partitioned Parquet."
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bench.name
        }
        Retry      = local.glue_retry
        Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "NotifyFailure" }]
        ResultPath = "$.bench"
        Next       = "PipelineSucceeded"
      }

      # -------------------------------------------------------------------
      # Failure branch: notify by email, then fail loudly.
      #
      # An earlier draft also emitted a custom "PipelineFailed" CloudWatch metric
      # here. That was removed after reconciling the cost estimate against
      # `terraform plan`: a custom metric costs $0.30/month standing, and
      # CloudWatch's own AWS/States ExecutionsFailed metric already fires on
      # exactly this condition for free. Publishing both meant paying $0.30/month
      # for a duplicate signal. The alarm now watches ExecutionsFailed instead.
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.alerts.arn
          Subject     = "hb-lakehouse pipeline FAILED"
          "Message.$" = "States.Format('hb-ecommerce-lakehouse pipeline failed.\n\nExecution: {}\nError: {}', $$.Execution.Name, $.error)"
        }
        ResultPath = null
        Next       = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more Glue jobs failed after retries. See the SNS notification and Glue job logs."
      }

      PipelineSucceeded = {
        Type = "Succeed"
      }
    }
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name       = "${var.project}-pipeline"
  role_arn   = aws_iam_role.stepfunctions.arn
  type       = "STANDARD"
  definition = local.pipeline_definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.states.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tags = merge(var.common_tags, { Name = "${var.project}-pipeline" })

  depends_on = [
    aws_iam_role_policy.stepfunctions,
    aws_budgets_budget.ceiling,
  ]
}
