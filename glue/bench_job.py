"""
Silver -> the three Phase 2 benchmark variants.

Owner: Hemanjali Buchireddy

This job exists so the benchmark is honest. It reads silver ONCE and writes the
SAME rows three ways:

    bench/raw_json/       JSON, UNCOMPRESSED, unpartitioned
    bench/parquet_flat/   Parquet + Snappy, unpartitioned
    bench/parquet_part/   Parquet + Snappy, partitioned by event_date

Because all three derive from one cached DataFrame in one job run, they are
guaranteed to hold identical logical rows. Building them separately, or at
different times, would leave the comparison open to the obvious objection that
the datasets differed.

What each variant isolates:

    raw_json      -> baseline: row-oriented, no compression, no pruning
    parquet_flat  -> the effect of columnar format + compression ALONE
    parquet_part  -> the additional effect of partition pruning on top

Why raw_json is uncompressed
----------------------------
Athena bills on bytes actually READ FROM S3. Gzipped JSON would report roughly
one seventh of its logical size, which would (a) collapse the measured spread
and (b) make every GB figure in BENCHMARK.md disagree with the
DataScannedInBytes that Athena actually reports. Uncompressed keeps the
comparison the textbook one and every number self-consistent, at a cost of about
$0.04/month in extra S3 storage.

Note this is a benchmark artifact only. The production bronze landing zone IS
gzipped, which is the correct choice there.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

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
TARGET = f"s3://{BUCKET}/{args['TARGET_PREFIX']}"

print(f"[bench] reading {SOURCE}")
silver = spark.read.parquet(SOURCE)

# Read once, materialize once, write three times. This is the whole point.
silver = silver.cache()
row_count = silver.count()
print(f"[bench] source rows: {row_count}")

if row_count == 0:
    print("[bench] silver is empty - exiting cleanly")
    job.commit()
    sys.exit(0)

# Identical column list across all three variants. event_date is a real column
# in the two unpartitioned variants and a partition key in the third; that
# difference IS the thing being measured.
COLUMNS = [
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
]

base = silver.select(*COLUMNS)

# File count is held constant across variants so the comparison measures format
# and pruning, not per-file overhead differences.
file_count = max(1, min(16, row_count // 400_000 + 1))

# ---------------------------------------------------------------------------
# 1. Raw JSON, uncompressed, unpartitioned
# ---------------------------------------------------------------------------
print(f"[bench] writing raw_json ({file_count} files, uncompressed)")
(
    base.coalesce(file_count)
    .write.mode("overwrite")
    .option("compression", "none")
    # Spark writes timestamps in JSON as ISO-8601 by default; pinning the format
    # keeps the JSON SerDe in the Glue table able to parse them.
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    .json(f"{TARGET}/raw_json/")
)

# ---------------------------------------------------------------------------
# 2. Parquet + Snappy, unpartitioned
# ---------------------------------------------------------------------------
print(f"[bench] writing parquet_flat ({file_count} files, snappy)")
(
    base.coalesce(file_count)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(f"{TARGET}/parquet_flat/")
)

# ---------------------------------------------------------------------------
# 3. Parquet + Snappy, partitioned by event_date
# ---------------------------------------------------------------------------
distinct_dates = base.select("event_date").distinct().count()
print(f"[bench] writing parquet_part ({distinct_dates} date partitions, snappy)")
(
    base.repartition("event_date")
    .write.mode("overwrite")
    .partitionBy("event_date")
    .option("compression", "snappy")
    .parquet(f"{TARGET}/parquet_part/")
)

print(
    f"[bench] done. {row_count} identical rows materialized three ways "
    f"across {distinct_dates} distinct event dates."
)

job.commit()
