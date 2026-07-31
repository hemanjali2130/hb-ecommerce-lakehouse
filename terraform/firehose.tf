# hb-ecommerce-lakehouse — ingestion
# Owner: Hemanjali Buchireddy
#
# Amazon Data Firehose, NOT Kinesis Data Streams. The deciding factor is idle
# cost, verified from the AWS Pricing API:
#
#   Firehose              $0.080 / GB ingested       $0.00 idle
#   KDS (provisioned)     $0.015 / shard-hour        $10.80 / month idle
#   KDS (on-demand)       $0.040 / stream-hour       $28.80 / month idle
#
# KDS buys multi-consumer fan-out, 24h-365d replay and per-shard ordering. This
# pipeline has one consumer and one destination and needs none of them, so KDS
# would be $10.80/month for nothing — more than twice the entire $5 ceiling.
# See ARCHITECTURE.md ADR-003.

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.project}-events"
  retention_in_days = var.log_retention_days
  tags              = var.common_tags
}

resource "aws_cloudwatch_log_stream" "firehose_s3" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_kinesis_firehose_delivery_stream" "events" {
  name        = "${var.project}-events"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.data.arn

    # Hive-style partitioning driven by Firehose's built-in timestamp namespace.
    # Deliberately NOT dynamic partitioning, which bills $0.02/GB plus $0.005 per
    # 1,000 objects; the ingest date is all bronze needs, and this form is free.
    prefix              = "${local.prefix_bronze}/ingest_date=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "firehose-errors/!{firehose:error-output-type}/ingest_date=!{timestamp:yyyy-MM-dd}/"

    # Buffering trade-off: larger buffers mean fewer, bigger objects — cheaper in
    # S3 PUTs and much faster to scan in Athena, since many small files are the
    # classic lakehouse performance killer. Smaller buffers mean fresher data on
    # the dashboard. 5 MB / 60 s keeps the live demo responsive; the benchmark
    # corpus is written in bulk mode and is unaffected by this setting.
    buffering_size     = 5
    buffering_interval = 60

    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3.name
    }

    processing_configuration {
      enabled = true

      processors {
        type = "Lambda"

        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.validator.arn}:$LATEST"
        }

        # Batch size for the transform. 3 MB is Firehose's maximum and keeps the
        # response comfortably under the 6 MB reply cap at ~400 bytes/event.
        parameters {
          parameter_name  = "BufferSizeInMBs"
          parameter_value = "3"
        }

        parameters {
          parameter_name  = "BufferIntervalInSeconds"
          parameter_value = "60"
        }

        # Retry a failed transform once before routing to the error prefix. The
        # handler writes quarantine objects under deterministic keys precisely so
        # this retry cannot duplicate them.
        parameters {
          parameter_name  = "NumberOfRetries"
          parameter_value = "1"
        }
      }
    }
  }

  tags = merge(var.common_tags, { Name = "${var.project}-events" })

  depends_on = [
    aws_iam_role_policy.firehose,
    aws_budgets_budget.ceiling,
  ]
}
