# hb-ecommerce-lakehouse — Glue catalog and PySpark jobs
# Owner: Hemanjali Buchireddy
#
# NO CRAWLER. Tables are declared explicitly here instead, for three reasons:
#   1. Cost — a crawler bills a 10-minute minimum per crawl at $0.44/DPU-hour
#      (~$0.15 every run) versus $0 for a static table definition.
#   2. Destroyability — a crawler-created table is not in Terraform state and
#      survives `terraform destroy`, leaving orphaned catalog entries.
#   3. Determinism — crawler schema inference guesses types from samples. An
#      explicit schema means silver genuinely enforces types rather than
#      re-inferring whatever happened to land that day.
#
# All three Glue jobs run execution_class = FLEX ($0.29/DPU-hour instead of
# $0.44, a 34% saving). Flex jobs can sit queued before starting, so there is
# deliberately NO task-level timeout in the state machine — the timeout lives on
# the Glue job itself, where queuing time is excluded. Retrying on a state
# machine timeout while the original run is still queued would double-execute.

locals {
  parquet_input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  json_input  = "org.apache.hadoop.mapred.TextInputFormat"
  json_output = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  json_serde  = "org.openx.data.jsonserde.JsonSerDe"

  # The canonical event shape, shared by silver and all three benchmark tables so
  # the Phase 2 comparison is genuinely like-for-like.
  event_columns = [
    { name = "event_id", type = "string" },
    { name = "event_type", type = "string" },
    { name = "event_timestamp", type = "timestamp" },
    { name = "customer_id", type = "string" },
    { name = "product_id", type = "string" },
    { name = "order_id", type = "string" },
    { name = "cart_id", type = "string" },
    { name = "quantity", type = "int" },
    { name = "unit_price", type = "double" },
    { name = "gross_amount", type = "double" },
    { name = "failure_code", type = "string" },
    { name = "is_late_arrival", type = "boolean" },
  ]

  # Gold star-schema column definitions.
  fact_orders_columns = [
    { name = "order_key", type = "string" },
    { name = "order_id", type = "string" },
    { name = "customer_key", type = "string" },
    { name = "product_key", type = "string" },
    { name = "date_key", type = "int" },
    { name = "quantity", type = "int" },
    { name = "unit_price", type = "double" },
    { name = "gross_amount", type = "double" },
    { name = "event_timestamp", type = "timestamp" },
    { name = "is_late_arrival", type = "boolean" },
  ]

  dim_customer_columns = [
    { name = "customer_key", type = "string" },
    { name = "customer_id", type = "string" },
    { name = "first_seen_at", type = "timestamp" },
    { name = "last_seen_at", type = "timestamp" },
    { name = "total_events", type = "bigint" },
    { name = "total_orders", type = "bigint" },
  ]

  dim_product_columns = [
    { name = "product_key", type = "string" },
    { name = "product_id", type = "string" },
    { name = "category", type = "string" },
    { name = "avg_unit_price", type = "double" },
    { name = "times_viewed", type = "bigint" },
    { name = "times_ordered", type = "bigint" },
  ]

  dim_date_columns = [
    { name = "date_key", type = "int" },
    { name = "full_date", type = "date" },
    { name = "year", type = "int" },
    { name = "quarter", type = "int" },
    { name = "month", type = "int" },
    { name = "day", type = "int" },
    { name = "day_of_week", type = "int" },
    { name = "day_name", type = "string" },
    { name = "is_weekend", type = "boolean" },
  ]
}

resource "aws_glue_catalog_database" "lakehouse" {
  name        = local.glue_database
  description = "hb-ecommerce-lakehouse — silver, gold star schema, and Phase 2 benchmark variants."
}

# ===========================================================================
# Job scripts — uploaded to the artifacts bucket
# ===========================================================================

resource "aws_s3_object" "silver_script" {
  bucket       = aws_s3_bucket.artifacts.id
  key          = "glue/silver_job.py"
  source       = "${path.module}/../glue/silver_job.py"
  etag         = filemd5("${path.module}/../glue/silver_job.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "gold_script" {
  bucket       = aws_s3_bucket.artifacts.id
  key          = "glue/gold_job.py"
  source       = "${path.module}/../glue/gold_job.py"
  etag         = filemd5("${path.module}/../glue/gold_job.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "bench_script" {
  bucket       = aws_s3_bucket.artifacts.id
  key          = "glue/bench_job.py"
  source       = "${path.module}/../glue/bench_job.py"
  etag         = filemd5("${path.module}/../glue/bench_job.py")
  content_type = "text/x-python"
}

# ===========================================================================
# Jobs
# ===========================================================================

locals {
  common_job_args = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--DATA_BUCKET"                      = aws_s3_bucket.data.id
    "--GLUE_DATABASE"                    = local.glue_database
    # Bookmarks are unavailable on Flex jobs, and the pipeline is a full
    # overwrite of silver/gold each run, so there is nothing to bookmark.
    "--job-bookmark-option" = "job-bookmark-disable"
  }
}

resource "aws_glue_job" "silver" {
  name        = "${var.project}-silver-job"
  description = "Bronze -> silver: dedupe on business key, enforce types, partition by event_date."
  role_arn    = aws_iam_role.glue_job.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_worker_count
  execution_class   = "FLEX"

  # Timeout belongs here, not on the Step Functions task: Glue's clock excludes
  # Flex queuing time, so this measures actual runtime.
  timeout     = 30
  max_retries = 0 # Step Functions owns retry policy.

  command {
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.silver_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.common_job_args, {
    "--SOURCE_PREFIX" = local.prefix_bronze
    "--TARGET_PREFIX" = local.prefix_silver
  })

  tags       = var.common_tags
  depends_on = [aws_budgets_budget.ceiling]
}

resource "aws_glue_job" "gold" {
  name        = "${var.project}-gold-job"
  description = "Silver -> gold: Kimball star schema (fact_orders + dim_customer/product/date)."
  role_arn    = aws_iam_role.glue_job.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_worker_count
  execution_class   = "FLEX"
  timeout           = 30
  max_retries       = 0

  command {
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.gold_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.common_job_args, {
    "--SOURCE_PREFIX"         = local.prefix_silver
    "--TARGET_PREFIX"         = local.prefix_gold
    "--EMIT_FRESHNESS_METRIC" = tostring(var.enable_freshness_metric)
    "--METRIC_NAMESPACE"      = "hb/lakehouse"
  })

  tags       = var.common_tags
  depends_on = [aws_budgets_budget.ceiling]
}

resource "aws_glue_job" "bench" {
  name        = "${var.project}-bench-job"
  description = "Materializes the SAME silver rows three ways for the Phase 2 benchmark."
  role_arn    = aws_iam_role.glue_job.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_worker_count
  execution_class   = "FLEX"
  timeout           = 30
  max_retries       = 0

  command {
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.bench_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.common_job_args, {
    "--SOURCE_PREFIX" = local.prefix_silver
    "--TARGET_PREFIX" = local.prefix_bench
  })

  tags       = var.common_tags
  depends_on = [aws_budgets_budget.ceiling]
}

# ===========================================================================
# Silver
# ===========================================================================

resource "aws_glue_catalog_table" "silver_events" {
  name          = "silver_events"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_silver}/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = local.event_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# ===========================================================================
# Gold — Kimball star schema
# ===========================================================================

resource "aws_glue_catalog_table" "fact_orders" {
  name          = "fact_orders"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_gold}/fact_orders/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = local.fact_orders_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "dim_customer" {
  name          = "dim_customer"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_gold}/dim_customer/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = local.dim_customer_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "dim_product" {
  name          = "dim_product"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_gold}/dim_product/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = local.dim_product_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "dim_date" {
  name          = "dim_date"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_gold}/dim_date/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = local.dim_date_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# ===========================================================================
# Benchmark variants — the Phase 2 deliverable
#
# Same logical rows, three physical layouts. Declared as first-class Terraform
# resources from the start so Phase 2 cannot arrive with nothing to compare.
#
# bench_raw_json is stored UNCOMPRESSED on purpose. Athena bills bytes actually
# read from S3, so a gzipped JSON table would report ~1/7th of its logical size
# and collapse the measured spread from ~200x to ~30x — while the printed GB
# figures silently disagreed with DataScannedInBytes. Uncompressed keeps the
# comparison the textbook one and every number self-consistent. It costs about
# $0.04/month in extra S3 storage.
# ===========================================================================

resource "aws_glue_catalog_table" "bench_raw_json" {
  name          = "bench_raw_json"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "json"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_bench}/raw_json/"
    input_format  = local.json_input
    output_format = local.json_output
    compressed    = false

    ser_de_info {
      serialization_library = local.json_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = concat(local.event_columns, [{ name = "event_date", type = "string" }])
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "bench_parquet_flat" {
  name          = "bench_parquet_flat"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  # No partition_keys: this variant isolates the effect of columnar format +
  # compression alone, with no partition pruning.
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_bench}/parquet_flat/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = concat(local.event_columns, [{ name = "event_date", type = "string" }])
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "bench_parquet_part" {
  name          = "bench_parquet_part"
  database_name = aws_glue_catalog_database.lakehouse.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  # Partitioned: isolates the additional effect of partition pruning on top of
  # the columnar format.
  partition_keys {
    name = "event_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/${local.prefix_bench}/parquet_part/"
    input_format  = local.parquet_input
    output_format = local.parquet_output
    compressed    = true

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters            = { "serialization.format" = "1" }
    }

    dynamic "columns" {
      for_each = local.event_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
