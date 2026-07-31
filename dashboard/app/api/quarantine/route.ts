/**
 * Quarantine counts broken down by rejection reason.
 *
 * Owner: Hemanjali Buchireddy
 *
 * Reads gold.quarantine_summary — an aggregate produced inside the pipeline —
 * NOT the raw quarantine prefix. hb-dashboard-reader has no S3 access to
 * quarantine/ at all, by design: those objects hold payloads that failed
 * validation and may contain malformed or unvalidated customer identifiers.
 * The dashboard gets counts and a sanitised sample detail, never a payload.
 */

import { NextResponse } from "next/server";

import { runQuery } from "@/lib/athena";

export const dynamic = "force-dynamic";

// Human-readable copy for each machine reason emitted by rules.py.
const REASON_COPY: Record<string, string> = {
  missing_required_field: "Missing a required field",
  missing_type_specific_field: "Missing a field required for that event type",
  invalid_json: "Payload was not parseable JSON",
  unknown_event_type: "Event type not in the accepted set",
  non_positive_quantity: "Order quantity was zero or negative",
  unparseable_timestamp: "Timestamp could not be parsed",
  non_numeric_amount: "Quantity or price was not numeric",
  malformed_event_id: "Event ID failed format check",
  future_timestamp: "Timestamp was implausibly in the future",
  negative_price: "Unit price was negative",
  bad_timestamp_type: "Timestamp was not a string",
  not_an_object: "Record was not a JSON object",
};

export async function GET() {
  const result = await runQuery(
    "quarantine:by-reason",
    `
    SELECT reject_reason,
           CAST(SUM(reject_count) AS VARCHAR) AS n,
           MAX(last_seen_at)                  AS last_seen_at
    FROM quarantine_summary
    GROUP BY reject_reason
    ORDER BY SUM(reject_count) DESC
    `,
  );

  if (result.status !== "ok") {
    return NextResponse.json(result, { status: result.status === "pending" ? 202 : 502 });
  }

  const reasons = result.rows.map((r) => ({
    reason: r.reject_reason,
    label: REASON_COPY[r.reject_reason] ?? r.reject_reason,
    count: Number(r.n ?? 0),
    lastSeenAt: r.last_seen_at ?? null,
  }));

  const total = reasons.reduce((sum, r) => sum + r.count, 0);

  return NextResponse.json({
    status: "ok",
    cached: result.cached,
    total,
    reasons: reasons.map((r) => ({
      ...r,
      sharePct: total > 0 ? (100 * r.count) / total : 0,
    })),
  });
}
