# hb-ecommerce-lakehouse — validator Lambda
# Owner: Hemanjali Buchireddy
#
# Packaged as a plain zip, not a container image: Docker is not installed on the
# build machine, and the handler needs nothing beyond the stdlib plus the boto3
# that the Python runtime already bundles. No build step, no wheels, no registry.

data "archive_file" "validator" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/validator"
  output_path = "${path.module}/.build/validator.zip"
  excludes    = ["__pycache__", "*.pyc"]
}

resource "aws_cloudwatch_log_group" "lambda_validator" {
  name              = "/aws/lambda/${var.project}-validator"
  retention_in_days = var.log_retention_days
  tags              = var.common_tags
}

resource "aws_lambda_function" "validator" {
  function_name = "${var.project}-validator"
  description   = "Firehose transform: schema-validate e-commerce events, quarantine rejects with a reason."

  role    = aws_iam_role.lambda_validator.arn
  handler = "handler.handler"
  runtime = "python3.12"

  filename         = data.archive_file.validator.output_path
  source_code_hash = data.archive_file.validator.output_base64sha256

  # Firehose gives the transform 5 minutes maximum. 60s is ample for parsing a
  # 500-record batch plus at most a handful of quarantine PUTs, and a low ceiling
  # means a hung invocation fails fast instead of burning GB-seconds.
  timeout = 60

  # 512 MB. Lambda bills GB-seconds, so memory and cost trade directly, but CPU
  # scales with memory too — under-provisioning here would cost MORE by running
  # proportionally longer.
  memory_size = 512

  environment {
    variables = {
      DATA_BUCKET       = aws_s3_bucket.data.id
      QUARANTINE_PREFIX = local.prefix_quarantine
    }
  }

  tags = merge(var.common_tags, { Name = "${var.project}-validator" })

  depends_on = [
    aws_iam_role_policy.lambda_validator,
    aws_cloudwatch_log_group.lambda_validator,
    aws_budgets_budget.ceiling,
  ]
}
