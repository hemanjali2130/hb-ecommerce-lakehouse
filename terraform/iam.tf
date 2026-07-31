# hb-ecommerce-lakehouse — least-privilege IAM
# Owner: Hemanjali Buchireddy
#
# One role per component. No role can do another role's job:
#
#   hb-firehose-role      writes ONLY to bronze/ and firehose-errors/, invokes
#                         ONLY the validator Lambda.
#   hb-lambda-validator   writes ONLY to quarantine/. It cannot write bronze —
#                         valid records go back to Firehose, never straight to S3.
#   hb-glue-job-role      reads bronze/silver, writes silver/gold/bench. Cannot
#                         touch quarantine (quarantine is an audit trail; a
#                         batch job has no business rewriting it).
#   hb-stepfunctions-role starts Glue jobs and publishes to SNS. No S3 access at
#                         all — it orchestrates, it never touches data.
#
# All inline policies (aws_iam_role_policy) rather than managed, because
# hb-builder-policy grants iam:PutRolePolicy on role/hb-* but managed policies
# would need separate policy/hb-* resources for no benefit at this size.

data "aws_iam_policy_document" "assume_firehose" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
    # Prevents the confused-deputy problem: this role can only be assumed on
    # behalf of Hemanjali's account, not by another customer's Firehose.
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [local.account_id]
    }
  }
}

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_glue" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_states" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# ===========================================================================
# 1. Firehose delivery role
# ===========================================================================

resource "aws_iam_role" "firehose" {
  name               = "${var.project}-firehose-role"
  description        = "Firehose delivery stream: land validated events in bronze, invoke the validator."
  assume_role_policy = data.aws_iam_policy_document.assume_firehose.json
  tags               = var.common_tags
}

data "aws_iam_policy_document" "firehose" {
  # Multipart upload to the two prefixes Firehose owns. Note the object-level
  # actions are scoped by prefix, not granted bucket-wide.
  statement {
    sid = "WriteBronzeAndErrors"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      aws_s3_bucket.data.arn,
      "${aws_s3_bucket.data.arn}/${local.prefix_bronze}/*",
      "${aws_s3_bucket.data.arn}/firehose-errors/*",
    ]
  }

  statement {
    sid       = "ListBucketScopedToOwnedPrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.prefix_bronze}/*", "firehose-errors/*"]
    }
  }

  # Exactly one function, by ARN. Not lambda:InvokeFunction on "*".
  statement {
    sid       = "InvokeValidatorOnly"
    actions   = ["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"]
    resources = [aws_lambda_function.validator.arn, "${aws_lambda_function.validator.arn}:*"]
  }

  statement {
    sid       = "DeliveryStreamLogging"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.project}-firehose-policy"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

# ===========================================================================
# 2. Lambda validator role
# ===========================================================================

resource "aws_iam_role" "lambda_validator" {
  name               = "${var.project}-lambda-validator-role"
  description        = "Firehose transform: schema-validate events and persist rejections to quarantine."
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = var.common_tags
}

data "aws_iam_policy_document" "lambda_validator" {
  # Quarantine ONLY. The validator physically cannot write to bronze, silver or
  # gold — if it could, a bug could corrupt the curated layers.
  statement {
    sid       = "WriteQuarantineOnly"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/${local.prefix_quarantine}/*"]
  }

  statement {
    sid       = "OwnLogsOnly"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda_validator.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda_validator" {
  name   = "${var.project}-lambda-validator-policy"
  role   = aws_iam_role.lambda_validator.id
  policy = data.aws_iam_policy_document.lambda_validator.json
}

# ===========================================================================
# 3. Glue job role
# ===========================================================================

resource "aws_iam_role" "glue_job" {
  name               = "${var.project}-glue-job-role"
  description        = "Glue PySpark jobs: build silver, gold and the three benchmark table variants."
  assume_role_policy = data.aws_iam_policy_document.assume_glue.json
  tags               = var.common_tags
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid     = "ReadBronzeSilverAndQuarantine"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "${aws_s3_bucket.data.arn}/${local.prefix_bronze}/*",
      "${aws_s3_bucket.data.arn}/${local.prefix_silver}/*",
      # READ ONLY. The gold job aggregates quarantine into a counts-only summary
      # so the dashboard never needs access to raw rejected payloads. Glue still
      # cannot WRITE to quarantine - it remains an append-only audit trail.
      "${aws_s3_bucket.data.arn}/${local.prefix_quarantine}/*",
    ]
  }

  # Writes the curated layers. Quarantine is intentionally absent — it is an
  # immutable audit trail written only by the validator.
  statement {
    sid     = "WriteCuratedLayers"
    actions = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.data.arn}/${local.prefix_silver}/*",
      "${aws_s3_bucket.data.arn}/${local.prefix_gold}/*",
      "${aws_s3_bucket.data.arn}/${local.prefix_bench}/*",
    ]
  }

  statement {
    sid       = "ListDataBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid       = "ReadJobScripts"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/*"]
  }

  # Catalog access scoped to this project's database and its tables only.
  statement {
    sid = "GlueCatalogForThisDatabaseOnly"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:UpdateTable",
      "glue:GetPartition", "glue:GetPartitions",
      "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${local.glue_database}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${local.glue_database}/*",
    ]
  }

  statement {
    sid       = "GlueJobLogging"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:AssociateKmsKey"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"]
  }

  # The gold job emits the data-freshness metric. Namespace-conditioned so a bug
  # cannot pollute AWS/* or any other namespace.
  statement {
    sid       = "EmitFreshnessMetricOnly"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["hb/lakehouse"]
    }
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "${var.project}-glue-job-policy"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# ===========================================================================
# 4. Step Functions role
# ===========================================================================

resource "aws_iam_role" "stepfunctions" {
  name               = "${var.project}-stepfunctions-role"
  description        = "Orchestrates the batch path. Starts Glue jobs and publishes failures. No data access."
  assume_role_policy = data.aws_iam_policy_document.assume_states.json
  tags               = var.common_tags
}

data "aws_iam_policy_document" "stepfunctions" {
  # Only this project's three jobs, by ARN.
  statement {
    sid     = "RunProjectGlueJobsOnly"
    actions = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [
      aws_glue_job.silver.arn,
      aws_glue_job.gold.arn,
      aws_glue_job.bench.arn,
    ]
  }

  statement {
    sid       = "PublishFailuresToProjectTopic"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }

  # NOTE: no cloudwatch:PutMetricData here. The state machine used to emit a
  # custom PipelineFailed metric; that was dropped in favour of the free
  # AWS/States ExecutionsFailed metric, so the grant is no longer needed. The
  # role is smaller as a direct result of a cost decision.

  # Step Functions Standard writes execution history to a vended log group.
  # These actions do not support resource-level permissions; AWS requires "*".
  statement {
    sid = "StateMachineLoggingRequiresWildcard"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
      "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "stepfunctions" {
  name   = "${var.project}-stepfunctions-policy"
  role   = aws_iam_role.stepfunctions.id
  policy = data.aws_iam_policy_document.stepfunctions.json
}
