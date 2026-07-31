# hb-ecommerce-lakehouse — provider configuration
# Owner: Hemanjali Buchireddy

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  # Hard stop if the resolved credentials point at any account other than
  # Hemanjali's. The Mac's `default` profile authenticates to a DIFFERENT account
  # (762233768052), so without this guard a forgotten --profile would silently
  # build the entire lakehouse in the wrong account.
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = var.common_tags
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  data_bucket      = "${var.bucket_prefix}-data"
  results_bucket   = "${var.bucket_prefix}-athena-results"
  artifacts_bucket = "${var.bucket_prefix}-artifacts"

  # S3 prefixes. Every stage of the medallion pipeline lives under one bucket so
  # IAM policies can scope to a single bucket ARN with prefix conditions.
  prefix_bronze     = "bronze"
  prefix_quarantine = "quarantine"
  prefix_silver     = "silver"
  prefix_gold       = "gold"
  prefix_bench      = "bench"

  glue_database = "hb_lakehouse"
}
