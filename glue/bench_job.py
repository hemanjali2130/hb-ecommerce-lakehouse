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

# event_date MUST be written as a string.
#
# Silver is written with partitionBy("event_date"), and when Spark reads a
# partitioned dataset back it INFERS the partition column's type — turning what
# was a string into DateType. In the partitioned variant that is invisible,
# because Athena reads partition values from the S3 path as strings. But in the
# two UNPARTITIONED variants event_date becomes a real column in the file, and
# Spark then stores it as INT32 (date), which does not match the `string` column
# declared in the Glue table. Athena rejects the file at query time:
#
#   HIVE_BAD_DATA: Malformed Parquet file. Field event_date's type INT32 ... is
#   incompatible with type varchar defined in table schema
#
# Casting here keeps the physical type identical across all three variants,
# which is exactly what a like-for-like benchmark requires.
base = base.withColumn("event_date", F.col("event_date").cast("string"))

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


# ---------------------------------------------------------------------------
# Register the partitions just written — via the Glue API, not MSCK REPAIR.
#
# The obvious approach, spark.sql("MSCK REPAIR TABLE ..."), needs Spark's Hive
# metastore client, which probes the `default` Glue database at session start and
# then tries to CREATE it when absent. That would require glue:CreateDatabase on
# the whole catalog — a far broader grant than partition registration warrants,
# and one that would let this role create arbitrary databases.
#
# Calling BatchCreatePartition directly uses only permissions the Glue role
# already holds (glue:GetTable, glue:BatchCreatePartition, s3:ListBucket), needs
# no `default` database, and works whether or not Spark is configured against the
# catalog. Explicit Glue tables never auto-discover partitions, so without this
# Athena returns zero rows from a prefix that visibly contains data.
# ---------------------------------------------------------------------------
def register_partitions(table, s3_prefix, partition_key="event_date"):
    import boto3
    from urllib.parse import urlparse

    database = args["GLUE_DATABASE"]
    glue = boto3.client("glue")
    s3c = boto3.client("s3")

    parsed = urlparse(s3_prefix)
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    values = set()
    for page in s3c.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            leaf = cp["Prefix"][len(prefix):].strip("/")
            if leaf.startswith(partition_key + "="):
                values.add(leaf.split("=", 1)[1])

    if not values:
        print(f"[partitions] {table}: no {partition_key}= directories under {s3_prefix}")
        return

    try:
        sd = glue.get_table(DatabaseName=database, Name=table)["Table"]["StorageDescriptor"]
    except Exception as exc:  # noqa: BLE001
        print(f"[partitions][WARN] {table}: cannot read table definition: {exc}")
        return

    inputs = []
    for v in sorted(values):
        part_sd = dict(sd)
        part_sd["Location"] = f"s3://{bucket}/{prefix}{partition_key}={v}/"
        inputs.append({"Values": [v], "StorageDescriptor": part_sd})

    registered = 0
    for i in range(0, len(inputs), 100):  # BatchCreatePartition caps at 100
        chunk = inputs[i:i + 100]
        try:
            resp = glue.batch_create_partition(
                DatabaseName=database, TableName=table, PartitionInputList=chunk
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[partitions][WARN] {table}: {exc}")
            return
        # AlreadyExists is the expected result on a re-run, not an error.
        real = [e for e in resp.get("Errors", [])
                if e.get("ErrorDetail", {}).get("ErrorCode") != "AlreadyExistsException"]
        if real:
            print(f"[partitions][WARN] {table}: {real[:3]}")
        registered += len(chunk)

    print(f"[partitions] {table}: {registered} partitions registered ({sorted(values)})")


register_partitions("bench_parquet_part", f"{TARGET}/parquet_part/")

print(
    f"[bench] done. {row_count} identical rows materialized three ways "
    f"across {distinct_dates} distinct event dates."
)

job.commit()
