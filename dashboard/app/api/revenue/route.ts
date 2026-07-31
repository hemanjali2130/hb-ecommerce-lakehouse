/**
 * Daily revenue from the gold star schema — the chart's data source.
 *
 * Owner: Hemanjali Buchireddy
 *
 * This is a real star-schema query, not a synthetic series: it joins fact_orders
 * to dim_date on the surrogate date_key, which is the join the whole Kimball
 * model exists to make cheap. Grouping by dim_date attributes rather than by
 * raw timestamps is what lets the chart show weekday/weekend structure without
 * date arithmetic at query time.
 *
 * fact_orders is partitioned by event_date, so this scans only the partitions
 * in range.
 */

import { NextResponse } from "next/server";

import { runQuery } from "@/lib/athena";

export const dynamic = "force-dynamic";

export async function GET() {
  const result = await runQuery(
    "revenue:daily-by-date-dim",
    `
    SELECT
      d.full_date                              AS day,
      d.day_name                               AS day_name,
      d.is_weekend                             AS is_weekend,
      CAST(COUNT(*) AS VARCHAR)                AS order_lines,
      CAST(ROUND(SUM(f.gross_amount), 2) AS VARCHAR) AS revenue,
      CAST(ROUND(AVG(f.gross_amount), 2) AS VARCHAR) AS avg_order_value
    FROM fact_orders f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.full_date, d.day_name, d.is_weekend
    ORDER BY d.full_date
    `,
  );

  if (result.status !== "ok") {
    return NextResponse.json(result, { status: result.status === "pending" ? 202 : 502 });
  }

  const series = result.rows.map((r) => ({
    day: r.day,
    dayName: r.day_name,
    isWeekend: r.is_weekend === "true",
    orderLines: Number(r.order_lines ?? 0),
    revenue: Number(r.revenue ?? 0),
    avgOrderValue: Number(r.avg_order_value ?? 0),
  }));

  return NextResponse.json({
    status: "ok",
    cached: result.cached,
    scannedBytes: result.scannedBytes,
    execMs: result.execMs,
    series,
    totals: {
      revenue: series.reduce((s, d) => s + d.revenue, 0),
      orderLines: series.reduce((s, d) => s + d.orderLines, 0),
      days: series.length,
    },
  });
}
