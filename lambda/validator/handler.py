"""
Firehose transform: schema-validate every e-commerce event in flight.

Owner: Hemanjali Buchireddy

Contract with Firehose
----------------------
Firehose hands us up to 500 records per invocation and expects one result per
record, keyed by recordId, with a status of Ok / Dropped / ProcessingFailed.

The hard requirement on this project is NEVER SILENTLY DROP. Firehose's own
`Dropped` status does exactly that: the record vanishes with no trace and no
reason. So the flow here is deliberately ordered:

    invalid record
      -> collect it, with the rejection reason attached
      -> write the whole invalid batch to s3://.../quarantine/ as ONE object
      -> ONLY after that PUT succeeds, return Dropped to Firehose

If the quarantine PUT fails we return ProcessingFailed instead, which routes the
record to Firehose's own error prefix. Either way the record is durable somewhere
before we tell Firehose to stop caring about it. There is no code path that
discards a record without persisting it first.

Three implementation details that matter
----------------------------------------
1. Deterministic quarantine keys. Firehose retries a failed invocation, and a
   retry must not create a second quarantine object holding the same records.
   The key is derived from the first recordId in the batch, so a retry overwrites
   its own previous attempt instead of duplicating it.

2. The 6 MB response cap. Firehose limits the transform response payload. We
   re-emit each valid record's original bytes unchanged, so the response is
   roughly the size of the input batch; at ~400 bytes/event x 500 records that
   is ~200 KB, comfortably inside the cap.

3. Pure stdlib + boto3. No third-party packages, so this builds as a plain zip
   with no Docker and no compiled wheels.

Partitioning
------------
Quarantine is partitioned by reject_reason, so the dashboard's "quarantine by
reason" query prunes to a single partition and costs Athena's 10 MB minimum
instead of scanning every rejected record ever written.
"""

import base64
import datetime as dt
import gzip
import json
import os
import re
from collections import defaultdict

import boto3

s3 = boto3.client("s3")

DATA_BUCKET = os.environ["DATA_BUCKET"]
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine")

# Event types the generator produces. Anything else is a rejection, not a silent pass.
VALID_EVENT_TYPES = {
    "order_placed",
    "cart_abandoned",
    "product_viewed",
    "payment_failed",
}

# Every event must carry these, regardless of type.
REQUIRED_FIELDS = ("event_id", "event_type", "event_timestamp", "customer_id")

# Fields required only for specific event types.
TYPE_REQUIRED_FIELDS = {
    "order_placed": ("order_id", "product_id", "quantity", "unit_price"),
    "cart_abandoned": ("cart_id", "product_id"),
    "product_viewed": ("product_id",),
    "payment_failed": ("order_id", "failure_code"),
}

UUID_RE = re.compile(r"^[0-9a-fA-F-]{8,36}$")

# How far ahead of now an event timestamp may sit before we treat it as a clock
# problem rather than a late arrival. Backdated events are legitimate here — the
# generator emits ~5% of them on purpose — so there is no lower bound.
MAX_FUTURE_SKEW_SECONDS = 300


class Rejection(Exception):
    """Carries a machine-readable reason used as the quarantine partition key."""

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}")


def _parse_timestamp(raw):
    """Accept ISO-8601 with or without trailing Z. Reject anything else."""
    if not isinstance(raw, str):
        raise Rejection("bad_timestamp_type", f"expected string, got {type(raw).__name__}")
    try:
        return dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise Rejection("unparseable_timestamp", raw[:80])


def validate(event: dict) -> dict:
    """Return the event annotated with derived fields, or raise Rejection."""

    if not isinstance(event, dict):
        raise Rejection("not_an_object", type(event).__name__)

    missing = [f for f in REQUIRED_FIELDS if f not in event or event[f] in (None, "")]
    if missing:
        raise Rejection("missing_required_field", ",".join(missing))

    event_type = event["event_type"]
    if event_type not in VALID_EVENT_TYPES:
        raise Rejection("unknown_event_type", str(event_type)[:80])

    type_missing = [
        f
        for f in TYPE_REQUIRED_FIELDS.get(event_type, ())
        if f not in event or event[f] in (None, "")
    ]
    if type_missing:
        raise Rejection("missing_type_specific_field", f"{event_type}:{','.join(type_missing)}")

    if not UUID_RE.match(str(event["event_id"])):
        raise Rejection("malformed_event_id", str(event["event_id"])[:80])

    ts = _parse_timestamp(event["event_timestamp"])
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)

    now = dt.datetime.now(dt.timezone.utc)
    if (ts - now).total_seconds() > MAX_FUTURE_SKEW_SECONDS:
        raise Rejection("future_timestamp", ts.isoformat())

    # Numeric sanity. A negative quantity or price is a real data-quality bug in
    # e-commerce feeds and is exactly the kind of thing silver dedup would happily
    # carry through to the gold fact table if we let it past here.
    if event_type == "order_placed":
        try:
            qty = int(event["quantity"])
            price = float(event["unit_price"])
        except (TypeError, ValueError):
            raise Rejection("non_numeric_amount", f"qty={event.get('quantity')} price={event.get('unit_price')}")
        if qty <= 0:
            raise Rejection("non_positive_quantity", str(qty))
        if price < 0:
            raise Rejection("negative_price", str(price))

    # Derived fields the Glue silver job relies on for partitioning.
    event["event_date"] = ts.date().isoformat()
    event["_validated_at"] = now.isoformat()
    event["_is_late_arrival"] = (now - ts).total_seconds() > 3600

    return event


def _quarantine_key(reason: str, batch_id: str, ingest_date: str) -> str:
    return (
        f"{QUARANTINE_PREFIX}/reject_reason={reason}/ingest_date={ingest_date}/"
        f"{batch_id}.jsonl.gz"
    )


def handler(event, context):
    records = event.get("records", [])
    now = dt.datetime.now(dt.timezone.utc)
    ingest_date = now.date().isoformat()

    # Deterministic across Firehose retries of this same batch.
    batch_id = records[0]["recordId"] if records else "empty"

    output = []
    rejected_by_reason = defaultdict(list)
    # recordId -> reason, so we can flip a record to ProcessingFailed if its
    # quarantine write later fails.
    rejected_record_ids = {}

    for rec in records:
        record_id = rec["recordId"]
        raw = base64.b64decode(rec["data"])

        try:
            parsed = json.loads(raw)
            validated = validate(parsed)

        except json.JSONDecodeError as exc:
            rejected_by_reason["invalid_json"].append(
                {
                    "reject_reason": "invalid_json",
                    "reject_detail": str(exc)[:200],
                    "rejected_at": now.isoformat(),
                    "raw_payload": raw.decode("utf-8", errors="replace")[:2000],
                }
            )
            rejected_record_ids[record_id] = "invalid_json"
            continue

        except Rejection as exc:
            rejected_by_reason[exc.reason].append(
                {
                    "reject_reason": exc.reason,
                    "reject_detail": exc.detail,
                    "rejected_at": now.isoformat(),
                    "raw_payload": raw.decode("utf-8", errors="replace")[:2000],
                }
            )
            rejected_record_ids[record_id] = exc.reason
            continue

        # Valid: hand the normalised event back to Firehose for delivery to bronze.
        # The trailing newline makes the delivered object newline-delimited JSON,
        # which is what the Glue silver job and the raw-JSON benchmark table read.
        payload = (json.dumps(validated, separators=(",", ":")) + "\n").encode("utf-8")
        output.append(
            {
                "recordId": record_id,
                "result": "Ok",
                "data": base64.b64encode(payload).decode("utf-8"),
            }
        )

    # ---------------------------------------------------------------------
    # Persist rejections BEFORE telling Firehose to drop them.
    # ---------------------------------------------------------------------
    failed_reasons = set()

    for reason, rows in rejected_by_reason.items():
        body = gzip.compress(
            ("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n").encode("utf-8")
        )
        try:
            s3.put_object(
                Bucket=DATA_BUCKET,
                Key=_quarantine_key(reason, batch_id, ingest_date),
                Body=body,
                ContentType="application/x-ndjson",
                ContentEncoding="gzip",
            )
        except Exception as exc:  # noqa: BLE001 - we must not lose the record
            # Could not quarantine. Do NOT drop. ProcessingFailed sends these to
            # Firehose's error output prefix, so they remain recoverable.
            print(f"[quarantine-failed] reason={reason} error={exc}")
            failed_reasons.add(reason)

    for record_id, reason in rejected_record_ids.items():
        output.append(
            {
                "recordId": record_id,
                "result": "ProcessingFailed" if reason in failed_reasons else "Dropped",
            }
        )

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "received": len(records),
                "valid": len(records) - len(rejected_record_ids),
                "quarantined": len(rejected_record_ids),
                "quarantine_write_failures": len(failed_reasons),
                "reasons": {k: len(v) for k, v in rejected_by_reason.items()},
            }
        )
    )

    return {"records": output}
