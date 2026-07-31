"""
Bronze -> Silver.

Owner: Hemanjali Buchireddy

Bronze is newline-delimited JSON, gzipped, written by Firehose and already
schema-validated in flight by the transform Lambda. Silver adds the three things
bronze cannot give you:

  1. Deduplication on the business key. At-least-once delivery is the normal
     guarantee for streaming ingestion, so the same event_id can and does land
     twice. Dedup here rather than in gold, so every downstream consumer sees
     one row per event without repeating the logic.

  2. Enforced types. Bronze JSON has everything as strings-or-whatever. Silver
     casts to a declared schema and NULLs what will not cast, rather than letting
     Athena guess per-file at query time.

  3. Snappy Parquet partitioned by event_date, which is what makes the Phase 2
     partition-pruning comparison possible at all.

Note on late arrivals: the generator emits ~5% of events with backdated
timestamps. Those are NOT errors and are NOT dropped — they are partitioned by
their true event_date, which is precisely why event_date and ingest_date differ
and why partitioning on event_date is the correct choice for analytics.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "DATA_BUCKET", "SOURCE_PREFIX", "TARGET_PREFIX", "GLUE_DATABASE"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET = args["DATA_BUCKET"]
SOURCE = f"s3://{BUCKET}/{args['SOURCE_PREFIX']}/"
TARGET = f"s3://{BUCKET}/{args['TARGET_PREFIX']}/"

# Explicit schema. Letting Spark infer would (a) cost a full extra pass over the
# data and (b) silently change column types between runs as the sample changes.
BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_timestamp", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("cart_id", T.StringType()),
        T.StructField("quantity", T.StringType()),
        T.StructField("unit_price", T.StringType()),
        T.StructField("failure_code", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("event_date", T.StringType()),
        T.StructField("_validated_at", T.StringType()),
        T.StructField("_is_late_arrival", T.BooleanType()),
    ]
)

print(f"[silver] reading {SOURCE}")
raw = spark.read.schema(BRONZE_SCHEMA).json(SOURCE)

total_in = raw.count()
print(f"[silver] bronze rows read: {total_in}")

if total_in == 0:
    print("[silver] nothing to process - exiting cleanly")
    job.commit()
    sys.exit(0)

# ---------------------------------------------------------------------------
# Type enforcement
# ---------------------------------------------------------------------------
typed = (
    raw.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))
    .withColumn("validated_at", F.to_timestamp("_validated_at"))
    .withColumn("is_late_arrival", F.coalesce(F.col("_is_late_arrival"), F.lit(False)))
)

# Rows whose timestamp would not cast cannot be partitioned by event_date and
# cannot be trusted downstream. The validator should have caught these, so
# reaching here means a gap in validation — surface the count rather than
# silently filtering.
unparseable = typed.filter(F.col("event_timestamp").isNull()).count()
if unparseable:
    print(f"[silver][WARN] {unparseable} rows had an uncastable event_timestamp and were excluded")
typed = typed.filter(F.col("event_timestamp").isNotNull())

# Derive event_date from the timestamp rather than trusting the field bronze
# carried, so the partition column and the timestamp can never disagree.
typed = typed.withColumn("event_date", F.date_format("event_timestamp", "yyyy-MM-dd"))

# gross_amount is computed once here so fact_orders and every benchmark variant
# share an identical definition.
typed = typed.withColumn(
    "gross_amount",
    F.when(
        F.col("event_type") == "order_placed",
        F.round(F.col("quantity") * F.col("unit_price"), 2),
    ).otherwise(F.lit(None).cast(T.DoubleType())),
)

# ---------------------------------------------------------------------------
# Deduplicate on the business key
#
# event_id is the business key. Where the same event_id appears more than once,
# keep the most recently validated copy. Deterministic tiebreak on _validated_at
# then event_timestamp so repeated runs produce byte-identical output.
# ---------------------------------------------------------------------------
dedupe_window = Window.partitionBy("event_id").orderBy(
    F.col("validated_at").desc_nulls_last(),
    F.col("event_timestamp").desc_nulls_last(),
)

deduped = (
    typed.withColumn("_rn", F.row_number().over(dedupe_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_validated_at", "_is_late_arrival", "validated_at")
)

total_out = deduped.count()
duplicates_removed = total_in - unparseable - total_out
print(f"[silver] rows after dedupe: {total_out}")
print(f"[silver] duplicates removed: {duplicates_removed}")

# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------
silver = deduped.select(
    "event_id",
    "event_type",
    "event_timestamp",
    "customer_id",
    "product_id",
    "order_id",
    "cart_id",
    "quantity",
    "unit_price",
    "gross_amount",
    "failure_code",
    "is_late_arrival",
    "event_date",
)

# Coalesce to bound the file count. Many tiny Parquet files are the classic
# lakehouse performance killer: Athena pays per-file overhead, so 500 x 1 MB
# files scan far slower than 5 x 100 MB files holding the same bytes.
partition_count = max(1, min(20, total_out // 200_000 + 1))

(
    silver.coalesce(partition_count)
    .write.mode("overwrite")
    .partitionBy("event_date")
    .option("compression", "snappy")
    .parquet(TARGET)
)

print(f"[silver] wrote {total_out} rows to {TARGET} in {partition_count} file(s) per partition")

job.commit()
