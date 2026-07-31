# hb-ecommerce-lakehouse — alarms
# Owner: Hemanjali Buchireddy
#
# COST WARNING: CloudWatch bills per ALARM-METRIC, not per alarm — $0.10 per
# alarm-metric per month, charged whether or not the alarm ever fires. These are
# among the very few resources in this project with a non-zero idle cost. The
# freshness alarm additionally needs a custom metric at $0.30/metric/month, which
# is why it sits behind var.enable_freshness_metric and can be switched off.
#
# Three alarms, as specified:
#   1. Lambda error rate      — validator failing
#   2. Glue job failure       — batch path broken
#   3. Data freshness lag     — pipeline silently stopped producing

# ---------------------------------------------------------------------------
# 1. Lambda error rate
#
# Expressed as a RATE, not a raw count. A raw-count alarm fires on a single
# transient error during a large batch; what actually matters is the proportion
# of invocations failing.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "lambda_error_rate" {
  alarm_name        = "${var.project}-validator-error-rate"
  alarm_description = "Validator Lambda failing on more than 5% of invocations over 10 minutes."

  comparison_operator = "GreaterThanThreshold"
  threshold           = 5
  evaluation_periods  = 2
  treat_missing_data  = "notBreaching" # No invocations is normal — the generator is off by default.

  # CloudWatch metric math cannot mix a time series and a scalar inside MAX(),
  # so guarding the divide-by-zero with MAX([invocations, 1]) is rejected at
  # PutMetricAlarm time. IF() is the supported form.
  metric_query {
    id          = "error_rate"
    expression  = "IF(invocations > 0, 100 * errors / invocations, 0)"
    label       = "Error rate (%)"
    return_data = true
  }

  metric_query {
    id = "errors"
    metric {
      namespace   = "AWS/Lambda"
      metric_name = "Errors"
      period      = 300
      stat        = "Sum"
      dimensions  = { FunctionName = aws_lambda_function.validator.function_name }
    }
  }

  metric_query {
    id = "invocations"
    metric {
      namespace   = "AWS/Lambda"
      metric_name = "Invocations"
      period      = 300
      stat        = "Sum"
      dimensions  = { FunctionName = aws_lambda_function.validator.function_name }
    }
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
  tags          = var.common_tags
}

# ---------------------------------------------------------------------------
# 2. Glue job failure
#
# ONE alarm on the custom PipelineFailed metric emitted by the Step Functions
# failure branch, rather than one alarm per Glue job. Three per-job alarms would
# be three alarm-metrics at $0.10 each; this is one, and it also catches
# orchestration failures that a per-job alarm would miss entirely.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "state_machine_failed" {
  alarm_name        = "${var.project}-state-machine-failed"
  alarm_description = "Step Functions execution failed — catches failures that never reached the PublishFailure state."

  namespace   = "AWS/States"
  metric_name = "ExecutionsFailed"
  statistic   = "Sum"
  period      = 300
  dimensions  = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }

  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = var.common_tags
}

# ---------------------------------------------------------------------------
# 3. Data freshness lag
#
# The gold job emits DataFreshnessLagSeconds = now - max(event_timestamp).
# This is the alarm that catches the nastiest failure mode: everything reports
# "success" but the data silently stopped moving.
#
# Optional because the metric costs $0.30/month standing. The dashboard computes
# freshness directly from Athena regardless; this exists purely so CloudWatch can
# alarm on it without anyone watching the dashboard.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "data_freshness" {
  count = var.enable_freshness_metric ? 1 : 0

  alarm_name        = "${var.project}-data-freshness-lag"
  alarm_description = "Gold layer max(event_timestamp) is more than 24h behind now — the pipeline has stalled."

  namespace   = "hb/lakehouse"
  metric_name = "DataFreshnessLagSeconds"
  statistic   = "Maximum"
  period      = 3600

  comparison_operator = "GreaterThanThreshold"
  threshold           = 86400 # 24 hours
  evaluation_periods  = 1

  # "missing" rather than "notBreaching": if the gold job stops running entirely
  # the metric stops arriving, and that is exactly the outage worth knowing about.
  treat_missing_data = "breaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
  tags          = var.common_tags
}
