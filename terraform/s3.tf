# hb-ecommerce-lakehouse — S3 storage layer
# Owner: Hemanjali Buchireddy
#
# Three buckets:
#   data      — bronze / quarantine / silver / gold / bench. The lakehouse itself.
#   results   — Athena query output. Athena requires a writable location.
#   artifacts — Lambda zips and Glue PySpark scripts.
#
# force_destroy = true on all three. Without it `terraform destroy` fails on any
# non-empty bucket and the "fully removable in one command" guarantee breaks.
# Acceptable here because every object is regenerable synthetic data.

# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket        = local.data_bucket
  force_destroy = true
  tags          = merge(var.common_tags, { Name = local.data_bucket, Layer = "lakehouse" })
}

resource "aws_s3_bucket" "results" {
  bucket        = local.results_bucket
  force_destroy = true
  tags          = merge(var.common_tags, { Name = local.results_bucket, Layer = "athena-results" })
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = local.artifacts_bucket
  force_destroy = true
  tags          = merge(var.common_tags, { Name = local.artifacts_bucket, Layer = "artifacts" })
}

# ---------------------------------------------------------------------------
# Public access — blocked on all three, no exceptions
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    data      = aws_s3_bucket.data.id
    results   = aws_s3_bucket.results.id
    artifacts = aws_s3_bucket.artifacts.id
  }

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Encryption — SSE-S3 (free). SSE-KMS would add $1/month per key plus per-request
# charges, which is real money against a $5 ceiling and buys nothing here:
# the data is synthetic and the threat model is public exposure, already covered
# by the public access block above.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = {
    data      = aws_s3_bucket.data.id
    results   = aws_s3_bucket.results.id
    artifacts = aws_s3_bucket.artifacts.id
  }

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# ---------------------------------------------------------------------------
# Lifecycle — the single largest recurring cost in this design is S3 storage,
# so both transient prefixes expire automatically.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id

  # Athena writes a .csv and a .metadata file for EVERY query, forever, and never
  # cleans up. Left alone this grows without bound.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {}

    expiration {
      days = 3
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  # Bronze is raw landed JSON. Once silver is built it is reproducible from the
  # generator, so it does not need to live forever.
  rule {
    id     = "expire-bronze"
    status = "Enabled"

    filter {
      prefix = "${local.prefix_bronze}/"
    }

    expiration {
      days = 14
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# ---------------------------------------------------------------------------
# Versioning is deliberately NOT enabled. Every noncurrent version bills at the
# full storage rate, and the pipeline overwrites silver/gold on every run — so
# versioning would silently multiply the storage bill by the number of runs.
# ---------------------------------------------------------------------------
