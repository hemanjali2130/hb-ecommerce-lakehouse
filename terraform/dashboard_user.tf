# hb-ecommerce-lakehouse — read-only identity for the Vercel dashboard
# Owner: Hemanjali Buchireddy
#
# This is the ONLY credential that ever leaves the build machine. It goes into
# Vercel's encrypted environment store and nowhere else.
#
# What it can do, exhaustively:
#   - run queries in the dashboard workgroup (which has a hard scan cutoff)
#   - read gold/ and bench/ objects
#   - read+write ONLY its own Athena results prefix
#   - read Step Functions execution history (for last-run status and duration)
#   - read catalog metadata for this database
#
# What it explicitly cannot do: read bronze, silver or quarantine raw data;
# write to any data prefix; start Glue jobs; use the benchmark workgroup (whose
# 10 GB cutoff would allow an expensive scan); touch anything outside this project.

resource "aws_iam_user" "dashboard_reader" {
  name = "${var.project}-dashboard-reader"
  tags = merge(var.common_tags, { Purpose = "vercel-dashboard-readonly" })
}

data "aws_iam_policy_document" "dashboard_reader" {
  # --- Athena: execute in the dashboard workgroup only ---
  statement {
    sid = "RunQueriesInDashboardWorkgroupOnly"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.dashboard.arn]
  }

  # --- Glue catalog: metadata only, this database only ---
  statement {
    sid     = "ReadCatalogMetadata"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${local.glue_database}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${local.glue_database}/*",
    ]
  }

  # --- S3: read gold and bench ONLY. Bronze, silver and quarantine are absent
  #     on purpose — the dashboard has no reason to read raw customer events. ---
  statement {
    sid     = "ReadCuratedDataOnly"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.data.arn}/${local.prefix_gold}/*",
      "${aws_s3_bucket.data.arn}/${local.prefix_bench}/*",
    ]
  }

  # GetBucketLocation must be UNCONDITIONAL. Athena calls it with no prefix while
  # verifying the query-results location, so folding it into the prefix-scoped
  # statement below makes every query fail with:
  #   "Unable to verify/create output bucket ..."
  # It leaks nothing — the answer is just the bucket's region.
  statement {
    sid       = "ResolveBucketRegions"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data.arn, aws_s3_bucket.results.arn]
  }

  # ListBucket on the data bucket stays prefix-scoped, so the dashboard can
  # resolve gold/ and bench/ table locations but cannot enumerate bronze,
  # silver or quarantine object keys.
  statement {
    sid       = "ListCuratedPrefixesOnly"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.prefix_gold}/*", "${local.prefix_bench}/*", "${local.prefix_gold}/", "${local.prefix_bench}/"]
    }
  }

  # --- Athena results: read/write its own prefix only ---
  statement {
    sid       = "OwnResultsPrefix"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.results.arn}/dashboard/*"]
  }

  # Unconditional on the results bucket: Athena lists it during output-location
  # verification without supplying a prefix. The bucket holds nothing but this
  # dashboard's own query results, and a 3-day lifecycle rule expires those.
  statement {
    sid       = "ListResultsBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.results.arn]
  }

  # --- Step Functions: read-only, for the "last pipeline run" panel ---
  statement {
    sid     = "ReadPipelineRunHistory"
    actions = ["states:ListExecutions", "states:DescribeExecution", "states:DescribeStateMachine"]
    resources = [
      aws_sfn_state_machine.pipeline.arn,
      "arn:aws:states:${local.region}:${local.account_id}:execution:${aws_sfn_state_machine.pipeline.name}:*",
    ]
  }

  # --- CloudWatch: read the freshness and throughput metrics ---
  statement {
    sid       = "ReadProjectMetrics"
    actions   = ["cloudwatch:GetMetricData", "cloudwatch:GetMetricStatistics"]
    resources = ["*"] # GetMetricData does not support resource-level permissions.
  }
}

resource "aws_iam_user_policy" "dashboard_reader" {
  name   = "${var.project}-dashboard-reader-policy"
  user   = aws_iam_user.dashboard_reader.name
  policy = data.aws_iam_policy_document.dashboard_reader.json
}

resource "aws_iam_access_key" "dashboard_reader" {
  user = aws_iam_user.dashboard_reader.name
}
