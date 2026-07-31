/**
 * Data freshness lag: now - max(event_timestamp) in the gold layer.
 *
 * Owner: Hemanjali Buchireddy
 *
 * This is the metric that catches the nastiest pipeline failure mode — every job
 * reports SUCCEEDED while the data silently stopped moving. Computed from Athena
 * here rather than read from the CloudWatch custom metric, so the dashboard keeps
 * working even with enable_freshness_metric = false.
 */

import { NextResponse } from "next/server";

import { runQuery } from "@/lib/athena";

export const dynamic = "force-dynamic";

export async function GET() {
  const result = await runQuery(
    "freshness:max-event-ts",
    `
    SELECT
      CAST(MAX(event_timestamp) AS VARCHAR)                                 AS max_event_ts,
      CAST(MIN(event_timestamp) AS VARCHAR)                                 AS min_event_ts,
      CAST(DATE_DIFF('second', MAX(event_timestamp), CURRENT_TIMESTAMP) AS VARCHAR) AS lag_seconds,
      CAST(COUNT(DISTINCT event_date) AS VARCHAR)                           AS distinct_days
    FROM fact_orders
    `,
  );

  if (result.status !== "ok") {
    return NextResponse.json(result, { status: result.status === "pending" ? 202 : 502 });
  }

  const r = result.rows[0] ?? {};
  const lagSeconds = Number(r.lag_seconds ?? 0);

  // Thresholds mirror the CloudWatch alarm (24h) so the UI and the alarm cannot
  // disagree about what "stale" means.
  const health = lagSeconds > 86_400 ? "stale" : lagSeconds > 21_600 ? "lagging" : "fresh";

  return NextResponse.json({
    status: "ok",
    cached: result.cached,
    maxEventTs: r.max_event_ts ?? null,
    minEventTs: r.min_event_ts ?? null,
    distinctDays: Number(r.distinct_days ?? 0),
    lagSeconds,
    health,
  });
}
