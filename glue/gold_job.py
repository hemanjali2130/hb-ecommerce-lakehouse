"""
Silver -> Gold: Kimball star schema.

Owner: Hemanjali Buchireddy

    fact_orders  (partitioned by event_date)
      |-- dim_customer
      |-- dim_product
      +-- dim_date

Design notes worth defending in an interview:

* The grain of fact_orders is ONE ROW PER ORDER LINE (order_id + product_id).
  Declaring the grain explicitly is the first thing Kimball asks for, and it is
  what makes the additive measures (quantity, gross_amount) safe to SUM.

* Surrogate keys, not natural keys, on the dimensions. customer_key is a hash of
  customer_id rather than customer_id itself. Real warehouses do this so the
  fact table is insulated from source-system key changes and so a slowly-changing
  dimension can later hold multiple versions of the same natural key.

* dim_date is generated from the observed date range rather than joined from the
  events. A date dimension must be dense — every day present whether or not it
  had orders — otherwise a "sales by day" report silently omits zero days.

* Only order_placed events become facts. Views, abandons and payment failures are
  different business processes at different grains; forcing them into one fact
  table is the classic beginner mistake.

This job also emits the data-freshness metric that CloudWatch alarms on.
"""

import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATA_BUCKET",
        "SOURCE_PREFIX",
        "TARGET_PREFIX",
        "GLUE_DATABASE",
        "EMIT_FRESHNESS_METRIC",
        "METRIC_NAMESPACE",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = args["DATA_BUCKET"]
SOURCE = f"s3://{BUCKET}/{args['SOURCE_PREFIX']}/"
TARGET = f"s3://{BUCKET}/{args['TARGET_PREFIX']}"
EMIT_METRIC = str(args["EMIT_FRESHNESS_METRIC"]).lower() == "true"
NAMESPACE = args["METRIC_NAMESPACE"]

print(f"[gold] reading {SOURCE}")
silver = spark.read.parquet(SOURCE)

row_count = silver.count()
print(f"[gold] silver rows: {row_count}")

if row_count == 0:
    print("[gold] silver is empty - exiting cleanly")
    job.commit()
    sys.exit(0)

silver = silver.cache()

# Surrogate keys. sha2(...) gives a stable, collision-resistant key that does not
# depend on load order — unlike monotonically_increasing_id(), which would change
# every run and break any downstream join.
customer_key = F.sha2(F.concat_ws("|", F.lit("cust"), F.col("customer_id")), 256).substr(1, 16)
product_key = F.sha2(F.concat_ws("|", F.lit("prod"), F.col("product_id")), 256).substr(1, 16)

# ---------------------------------------------------------------------------
# dim_customer
# ---------------------------------------------------------------------------
dim_customer = (
    silver.filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.min("event_timestamp").alias("first_seen_at"),
        F.max("event_timestamp").alias("last_seen_at"),
        F.count(F.lit(1)).alias("total_events"),
        F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).cast(T.LongType()).alias("total_orders"),
    )
    .withColumn("customer_key", F.sha2(F.concat_ws("|", F.lit("cust"), F.col("customer_id")), 256).substr(1, 16))
    .select("customer_key", "customer_id", "first_seen_at", "last_seen_at", "total_events", "total_orders")
)

# ---------------------------------------------------------------------------
# dim_product
# ---------------------------------------------------------------------------
dim_product = (
    silver.filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        F.round(F.avg(F.when(F.col("unit_price") > 0, F.col("unit_price"))), 2).alias("avg_unit_price"),
        F.sum(F.when(F.col("event_type") == "product_viewed", 1).otherwise(0)).cast(T.LongType()).alias("times_viewed"),
        F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).cast(T.LongType()).alias("times_ordered"),
    )
    # Category is derived from the product_id prefix the generator uses, so the
    # dimension carries a genuine descriptive attribute rather than only measures.
    .withColumn("category", F.split(F.col("product_id"), "-").getItem(0))
    .withColumn("product_key", F.sha2(F.concat_ws("|", F.lit("prod"), F.col("product_id")), 256).substr(1, 16))
    .select("product_key", "product_id", "category", "avg_unit_price", "times_viewed", "times_ordered")
)

# ---------------------------------------------------------------------------
# dim_date — dense across the full observed range, including days with no events
# ---------------------------------------------------------------------------
bounds = silver.select(
    F.min(F.to_date("event_timestamp")).alias("min_d"),
    F.max(F.to_date("event_timestamp")).alias("max_d"),
).collect()[0]

dim_date = (
    spark.sql(f"SELECT sequence(to_date('{bounds['min_d']}'), to_date('{bounds['max_d']}'), interval 1 day) AS days")
    .select(F.explode("days").alias("full_date"))
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(T.IntegerType()))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
    .select("date_key", "full_date", "year", "quarter", "month", "day", "day_of_week", "day_name", "is_weekend")
)

# ---------------------------------------------------------------------------
# fact_orders — grain: one row per order line
# ---------------------------------------------------------------------------
fact_orders = (
    silver.filter(F.col("event_type") == "order_placed")
    .withColumn("customer_key", customer_key)
    .withColumn("product_key", product_key)
    .withColumn("date_key", F.date_format("event_timestamp", "yyyyMMdd").cast(T.IntegerType()))
    .withColumn(
        "order_key",
        F.sha2(F.concat_ws("|", F.col("order_id"), F.col("product_id")), 256).substr(1, 16),
    )
    .select(
        "order_key",
        "order_id",
        "customer_key",
        "product_key",
        "date_key",
        "quantity",
        "unit_price",
        "gross_amount",
        "event_timestamp",
        "is_late_arrival",
        "event_date",
    )
)

fact_count = fact_orders.count()
print(f"[gold] fact_orders rows: {fact_count}")

# ---------------------------------------------------------------------------
# Write. Dimensions are small — one file each keeps Athena's per-file overhead
# negligible on the joins.
# ---------------------------------------------------------------------------
dim_customer.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{TARGET}/dim_customer/")
dim_product.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{TARGET}/dim_product/")
dim_date.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{TARGET}/dim_date/")

fact_partitions = max(1, min(20, fact_count // 200_000 + 1))
(
    fact_orders.coalesce(fact_partitions)
    .write.mode("overwrite")
    .partitionBy("event_date")
    .option("compression", "snappy")
    .parquet(f"{TARGET}/fact_orders/")
)

print(f"[gold] star schema written to {TARGET}")

# ---------------------------------------------------------------------------
# Register the partitions just written.
#
# The Glue tables are declared explicitly in Terraform (no crawler), so nothing
# discovers new event_date=... directories on its own. Without this, Athena
# queries a table whose S3 location is full of data and returns zero rows.
# ---------------------------------------------------------------------------
def repair(table):
    try:
        spark.sql(f"MSCK REPAIR TABLE `{args['GLUE_DATABASE']}`.`{table}`")
        n = spark.sql(f"SHOW PARTITIONS `{args['GLUE_DATABASE']}`.`{table}`").count()
        print(f"[partitions] {table}: {n} registered")
    except Exception as exc:  # noqa: BLE001
        print(f"[partitions][WARN] {table}: {exc}")

repair("fact_orders")


# ---------------------------------------------------------------------------
# Data freshness metric
#
# now - max(event_timestamp) in gold. This is the signal that catches the worst
# failure mode in a pipeline: every job reports success while the data silently
# stopped moving.
# ---------------------------------------------------------------------------
if EMIT_METRIC:
    max_event_ts = fact_orders.select(F.max("event_timestamp")).collect()[0][0]
    if max_event_ts is not None:
        lag_seconds = (datetime.now(timezone.utc) - max_event_ts.replace(tzinfo=timezone.utc)).total_seconds()
        lag_seconds = max(0.0, lag_seconds)

        boto3.client("cloudwatch").put_metric_data(
            Namespace=NAMESPACE,
            # ONE custom metric, deliberately. CloudWatch bills $0.30 per custom
            # metric per month whether or not anything reads it, so a second
            # "GoldFactRowCount" metric was dropped — the dashboard already gets
            # row counts from Athena for free.
            MetricData=[
                {"MetricName": "DataFreshnessLagSeconds", "Value": lag_seconds, "Unit": "Seconds"},
            ],
        )
        print(f"[gold] freshness lag emitted: {lag_seconds:.0f}s (max event ts {max_event_ts})")
    else:
        print("[gold] no fact rows - freshness metric not emitted")
else:
    print("[gold] freshness metric disabled by configuration")

# ---------------------------------------------------------------------------
# Quarantine summary
#
# The dashboard needs "quarantine counts broken down by rejection reason", but
# hb-dashboard-reader is deliberately NOT granted read access to the quarantine
# prefix: those objects contain the raw rejected payloads, which by definition
# never passed validation and may carry malformed or unvalidated customer
# identifiers. Exposing them to a public web dashboard would be wrong.
#
# So the aggregate is computed here, inside the trusted pipeline, and published
# to gold/ as counts only — no payloads. The dashboard reads the summary.
# ---------------------------------------------------------------------------
QUARANTINE = f"s3://{BUCKET}/{args['QUARANTINE_PREFIX']}/"
try:
    quarantine = spark.read.option("basePath", QUARANTINE).json(QUARANTINE)

    summary = (
        quarantine.groupBy("reject_reason", "ingest_date")
        .agg(
            F.count(F.lit(1)).cast(T.LongType()).alias("reject_count"),
            F.max("rejected_at").alias("last_seen_at"),
            # One representative detail per reason, so the dashboard can show
            # WHY without ever surfacing a raw payload.
            F.first("reject_detail", ignorenulls=True).alias("sample_detail"),
        )
        .orderBy(F.col("reject_count").desc())
    )

    summary.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
        f"{TARGET}/quarantine_summary/"
    )
    total_rejects = summary.agg(F.sum("reject_count")).collect()[0][0] or 0
    print(f"[gold] quarantine_summary written: {total_rejects} rejected rows across "
          f"{summary.count()} (reason, date) pairs")

except Exception as exc:  # noqa: BLE001
    # An empty quarantine prefix is a legitimate state (nothing has been
    # rejected yet), not a pipeline failure. Write an empty summary so the
    # dashboard query still resolves against a real table.
    print(f"[gold] quarantine summary skipped: {exc}")
    empty_schema = T.StructType([
        T.StructField("reject_reason", T.StringType()),
        T.StructField("ingest_date", T.StringType()),
        T.StructField("reject_count", T.LongType()),
        T.StructField("last_seen_at", T.StringType()),
        T.StructField("sample_detail", T.StringType()),
    ])
    spark.createDataFrame([], empty_schema).coalesce(1).write.mode("overwrite").option(
        "compression", "snappy"
    ).parquet(f"{TARGET}/quarantine_summary/")

job.commit()
