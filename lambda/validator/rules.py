"""
Shared schema-validation rules.

Owner: Hemanjali Buchireddy

This module is the single definition of what a valid e-commerce event looks
like. It is imported by BOTH transport paths:

  * lambda/validator/handler.py  — the Firehose transform (stream mode)
  * generator/generate.py        — direct-to-S3 writes (bulk mode)

Keeping one implementation matters. If bulk mode had its own copy, the benchmark
corpus could drift from the streamed data and the quarantine counts on the
dashboard would describe two different rulesets. Worse, bulk records would reach
bronze unvalidated and the silver job would drop the malformed ones silently —
exactly what this project promises never to do.

Deliberately stdlib-only, no boto3, no env vars read at import. That keeps it
importable from a plain test process and keeps the Lambda zip dependency-free.
"""

import datetime as dt
import re

VALID_EVENT_TYPES = {
    "order_placed",
    "cart_abandoned",
    "product_viewed",
    "payment_failed",
}

REQUIRED_FIELDS = ("event_id", "event_type", "event_timestamp", "customer_id")

TYPE_REQUIRED_FIELDS = {
    "order_placed": ("order_id", "product_id", "quantity", "unit_price"),
    "cart_abandoned": ("cart_id", "product_id"),
    "product_viewed": ("product_id",),
    "payment_failed": ("order_id", "failure_code"),
}

UUID_RE = re.compile(r"^[0-9a-fA-F-]{8,36}$")

# Events may be arbitrarily far in the PAST — roughly 5% are deliberately
# backdated to simulate offline mobile clients, and those are valid. Only future
# timestamps beyond a small skew allowance indicate a broken clock.
MAX_FUTURE_SKEW_SECONDS = 300

# Anything older than this is flagged (not rejected) as a late arrival.
LATE_ARRIVAL_THRESHOLD_SECONDS = 3600


class Rejection(Exception):
    """Carries a machine-readable reason, used as the quarantine partition key."""

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}")


def _parse_timestamp(raw):
    if not isinstance(raw, str):
        raise Rejection("bad_timestamp_type", f"expected string, got {type(raw).__name__}")
    try:
        return dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise Rejection("unparseable_timestamp", raw[:80])


def validate(event, now=None):
    """
    Return the event annotated with derived fields, or raise Rejection.

    `now` is injectable so tests are deterministic.
    """
    now = now or dt.datetime.now(dt.timezone.utc)

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

    if (ts - now).total_seconds() > MAX_FUTURE_SKEW_SECONDS:
        raise Rejection("future_timestamp", ts.isoformat())

    # Numeric sanity. A negative quantity is a genuine data-quality defect in
    # real order feeds and would otherwise flow straight through dedup into the
    # gold fact table, where it would quietly corrupt revenue sums.
    if event_type == "order_placed":
        try:
            qty = int(event["quantity"])
            price = float(event["unit_price"])
        except (TypeError, ValueError):
            raise Rejection(
                "non_numeric_amount",
                f"qty={event.get('quantity')} price={event.get('unit_price')}",
            )
        if qty <= 0:
            raise Rejection("non_positive_quantity", str(qty))
        if price < 0:
            raise Rejection("negative_price", str(price))

    event["event_date"] = ts.date().isoformat()
    event["_validated_at"] = now.isoformat()
    event["_is_late_arrival"] = (now - ts).total_seconds() > LATE_ARRIVAL_THRESHOLD_SECONDS

    return event
