#!/usr/bin/env python3
"""
Synthetic e-commerce event generator.

Owner: Hemanjali Buchireddy

OFF BY DEFAULT. Nothing runs unless you invoke this script explicitly. It is the
only component that puts data (and therefore cost) into the pipeline.

Two modes, and the difference matters:

  stream  Publishes through Amazon Data Firehose, so records pass through the
          real validator Lambda and land in bronze exactly as production traffic
          would. Bills $0.080/GB. Use this to demonstrate the live path.

  bulk    Writes the same records straight to S3 with multipart uploads,
          bypassing Firehose entirely. Used to build the ~2 GB Phase 2 benchmark
          corpus quickly and without paying Firehose ingest on data whose only
          purpose is to be scanned by Athena.

Running both is deliberate, not a shortcut: it is what makes the
"Kinesis vs direct S3 writes" ADR an empirical comparison rather than a
hypothetical one. BENCHMARK.md states plainly which corpus came from which path.

Deliberate data-quality defects
-------------------------------
  ~2% malformed  - missing required fields, wrong types, unparseable JSON,
                   negative quantities, unknown event types. These exist to
                   prove the quarantine path works; every one should surface in
                   the dashboard broken down by rejection reason.

  ~5% late       - backdated by 2-72 hours. These are NOT errors. They are the
                   normal consequence of mobile clients buffering offline, and
                   they are exactly why the pipeline partitions on event_date
                   (when it happened) rather than ingest_date (when we saw it).

Usage
-----
  python3 generate.py --mode stream --rate 200 --duration 60
  python3 generate.py --mode bulk --target-gb 2
  python3 generate.py --mode bulk --target-gb 0.01 --dry-run
"""

import argparse
import datetime as dt
import gzip
import io
import json
import os
import random
import string
import sys
import time
import uuid

# boto3 is imported lazily inside run_stream/run_bulk rather than at module load,
# so the event-shaping logic and the defect rates can be unit-tested with plain
# system Python and no AWS dependency installed.

# ---------------------------------------------------------------------------
# Reference data — small enough to give realistic cardinality, large enough that
# dim_customer and dim_product are not trivially tiny.
# ---------------------------------------------------------------------------

CATEGORIES = ["electronics", "apparel", "home", "grocery", "beauty", "sports", "toys", "books"]

EVENT_WEIGHTS = {
    "product_viewed": 0.62,   # Views dominate real funnels by a wide margin.
    "cart_abandoned": 0.18,   # Roughly 70% of carts are abandoned in practice.
    "order_placed": 0.17,
    "payment_failed": 0.03,
}

FAILURE_CODES = [
    "insufficient_funds",
    "card_expired",
    "cvv_mismatch",
    "issuer_declined",
    "fraud_suspected",
    "network_timeout",
]

N_CUSTOMERS = 5_000
N_PRODUCTS = 800

CUSTOMERS = [f"cust-{i:06d}" for i in range(N_CUSTOMERS)]
PRODUCTS = [
    (f"{random.choice(CATEGORIES)}-{i:05d}", round(random.uniform(4.99, 899.99), 2))
    for i in range(N_PRODUCTS)
]

MALFORMED_RATE = 0.02
LATE_RATE = 0.05


def _now():
    return dt.datetime.now(dt.timezone.utc)


def make_event(force_type=None):
    """Produce one well-formed event."""
    event_type = force_type or random.choices(
        list(EVENT_WEIGHTS), weights=list(EVENT_WEIGHTS.values())
    )[0]

    product_id, base_price = random.choice(PRODUCTS)
    customer_id = random.choice(CUSTOMERS)

    ts = _now()
    is_late = random.random() < LATE_RATE
    if is_late:
        # Backdated 2-72 hours: a phone that was offline, not a bug.
        ts -= dt.timedelta(hours=random.uniform(2, 72))

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": ts.isoformat().replace("+00:00", "Z"),
        "customer_id": customer_id,
        "product_id": product_id,
        "category": product_id.split("-")[0],
    }

    if event_type == "order_placed":
        event["order_id"] = f"ord-{uuid.uuid4().hex[:12]}"
        # Quantity is heavily skewed toward 1, as real baskets are.
        event["quantity"] = random.choices([1, 2, 3, 4, 5], weights=[70, 18, 7, 3, 2])[0]
        event["unit_price"] = round(base_price * random.uniform(0.85, 1.0), 2)

    elif event_type == "cart_abandoned":
        event["cart_id"] = f"cart-{uuid.uuid4().hex[:12]}"
        event["quantity"] = random.randint(1, 4)
        event["unit_price"] = base_price

    elif event_type == "payment_failed":
        event["order_id"] = f"ord-{uuid.uuid4().hex[:12]}"
        event["failure_code"] = random.choice(FAILURE_CODES)
        event["quantity"] = random.randint(1, 3)
        event["unit_price"] = base_price

    return event


def corrupt(event):
    """
    Damage an event in one of several realistic ways.

    Each corruption maps to a distinct rejection reason in the validator, so the
    dashboard's quarantine breakdown has real variety rather than one bucket.
    """
    mode = random.choice(
        [
            "drop_required",
            "bad_type",
            "unknown_event_type",
            "negative_quantity",
            "unparseable_timestamp",
            "malformed_json",
            "null_customer",
        ]
    )

    if mode == "drop_required":
        for field in random.sample(["event_id", "event_type", "customer_id", "event_timestamp"], 1):
            event.pop(field, None)

    elif mode == "bad_type":
        event["quantity"] = "".join(random.choices(string.ascii_letters, k=5))

    elif mode == "unknown_event_type":
        event["event_type"] = random.choice(["wishlist_added", "page_scrolled", "unknown", ""])

    elif mode == "negative_quantity":
        event["event_type"] = "order_placed"
        event.setdefault("order_id", f"ord-{uuid.uuid4().hex[:12]}")
        event["quantity"] = -random.randint(1, 5)
        event.setdefault("unit_price", 19.99)

    elif mode == "unparseable_timestamp":
        event["event_timestamp"] = random.choice(
            ["not-a-date", "2026-13-45T99:99:99Z", "", "0000-00-00"]
        )

    elif mode == "null_customer":
        event["customer_id"] = None

    elif mode == "malformed_json":
        # Returned as a raw string: genuinely unparseable, not just invalid.
        return '{"event_id": "' + str(uuid.uuid4()) + '", "event_type": "order_placed", TRUNCATED'

    return event


def serialize(event):
    if isinstance(event, str):
        return event  # already-broken JSON
    return json.dumps(event, separators=(",", ":"))


def emit_batch(n):
    """Yield n serialized records with the configured defect rates applied."""
    out = []
    for _ in range(n):
        ev = make_event()
        if random.random() < MALFORMED_RATE:
            ev = corrupt(ev)
        out.append(serialize(ev))
    return out


# ---------------------------------------------------------------------------
# Stream mode — through Firehose, exercising the real validator
# ---------------------------------------------------------------------------

def run_stream(stream_name, rate, duration, region, profile):
    import boto3

    session = boto3.Session(profile_name=profile, region_name=region)
    fh = session.client("firehose")

    print(f"[stream] -> {stream_name} @ ~{rate} events/sec for {duration}s")
    print(f"[stream] defects: {MALFORMED_RATE:.0%} malformed, {LATE_RATE:.0%} late-arriving")

    sent = failed = 0
    started = time.time()
    deadline = started + duration

    while time.time() < deadline:
        tick = time.time()

        # Firehose PutRecordBatch caps at 500 records / 4 MB per call.
        remaining = rate
        while remaining > 0:
            n = min(500, remaining)
            records = [{"Data": (r + "\n").encode("utf-8")} for r in emit_batch(n)]

            try:
                resp = fh.put_record_batch(DeliveryStreamName=stream_name, Records=records)
                failed += resp.get("FailedPutCount", 0)
                sent += n - resp.get("FailedPutCount", 0)
            except Exception as exc:  # noqa: BLE001
                print(f"[stream][ERROR] {exc}")
                failed += n

            remaining -= n

        elapsed = time.time() - started
        print(f"\r[stream] sent={sent} failed={failed} elapsed={elapsed:.0f}s", end="", flush=True)

        # Pace to roughly one batch per second.
        sleep_for = 1.0 - (time.time() - tick)
        if sleep_for > 0:
            time.sleep(sleep_for)

    print(f"\n[stream] done. sent={sent} failed={failed} in {time.time() - started:.0f}s")
    approx_gb = sent * 400 / 1e9
    print(f"[stream] approx {approx_gb:.4f} GB ingested -> ~${approx_gb * 0.08:.4f} Firehose cost")


# ---------------------------------------------------------------------------
# Bulk mode — straight to S3, for the benchmark corpus
# ---------------------------------------------------------------------------

def run_bulk(bucket, prefix, target_gb, region, profile, dry_run=False):
    import boto3

    session = boto3.Session(profile_name=profile, region_name=region)
    s3 = session.client("s3")

    target_bytes = int(target_gb * 1e9)
    # ~24 MB gzipped objects: big enough that Athena is not paying per-file
    # overhead, small enough to stream without buffering gigabytes in memory.
    rows_per_object = 60_000

    print(f"[bulk] target {target_gb} GB -> s3://{bucket}/{prefix}/")
    if dry_run:
        print("[bulk] DRY RUN - nothing will be written")

    written_bytes = 0
    obj_index = 0
    started = time.time()
    ingest_date = _now().date().isoformat()

    while written_bytes < target_bytes:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            chunk = "\n".join(emit_batch(rows_per_object)) + "\n"
            raw = chunk.encode("utf-8")
            gz.write(raw)

        body = buf.getvalue()
        # Count UNCOMPRESSED bytes toward the target: the benchmark cares about
        # logical corpus size, and bench_raw_json is written uncompressed.
        written_bytes += len(raw)
        key = f"{prefix}/ingest_date={ingest_date}/bulk-{obj_index:05d}.jsonl.gz"

        if not dry_run:
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")

        obj_index += 1
        pct = 100 * written_bytes / target_bytes
        print(
            f"\r[bulk] {obj_index} objects, {written_bytes / 1e9:.3f}/{target_gb} GB ({pct:.0f}%)",
            end="",
            flush=True,
        )

    elapsed = time.time() - started
    print(f"\n[bulk] done. {obj_index} objects, {written_bytes / 1e9:.3f} GB in {elapsed:.0f}s")
    print(f"[bulk] S3 storage cost: ~${written_bytes / 1e9 * 0.023:.4f}/month (compressed on disk)")
    print("[bulk] Firehose cost avoided by using bulk mode: "
          f"~${written_bytes / 1e9 * 0.08:.4f}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["stream", "bulk"], required=True)
    p.add_argument("--profile", default=os.environ.get("AWS_PROFILE", "hb"))
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))

    p.add_argument("--stream-name", default="hb-events", help="Firehose delivery stream (stream mode)")
    p.add_argument("--rate", type=int, default=200, help="events/sec (stream mode)")
    p.add_argument("--duration", type=int, default=60, help="seconds to run (stream mode)")

    p.add_argument("--bucket", help="Data bucket (bulk mode)")
    p.add_argument("--prefix", default="bronze", help="S3 prefix (bulk mode)")
    p.add_argument("--target-gb", type=float, default=2.0, help="Corpus size (bulk mode)")
    p.add_argument("--dry-run", action="store_true")

    p.add_argument("--seed", type=int, help="Seed the RNG for reproducible corpora")

    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.mode == "stream":
        run_stream(args.stream_name, args.rate, args.duration, args.region, args.profile)
    else:
        if not args.bucket:
            p.error("--bucket is required in bulk mode")
        run_bulk(args.bucket, args.prefix, args.target_gb, args.region, args.profile, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
