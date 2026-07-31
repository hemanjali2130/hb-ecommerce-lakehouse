/**
 * Per-stage record counts.
 *
 * Owner: Hemanjali Buchireddy
 *
 * Every table referenced here is partitioned or tiny, so the whole route costs
 * Athena's 10 MB per-query minimum. The workgroup's 200 MB scan cutoff is the
 * hard backstop underneath.
 */

import { NextResponse } from "next/server";

import { runQuery } from "@/lib/athena";

// Never statically rendered — this reports live pipeline state.
export const dynamic = "force-dynamic";

export async function GET() {
  const result = await runQuery(
    "metrics:stage-counts",
    `
    SELECT
      (SELECT COUNT(*) FROM fact_orders)                      AS gold_fact_rows,
      (SELECT COUNT(DISTINCT customer_key) FROM dim_customer) AS gold_customers,
      (SELECT COUNT(DISTINCT product_key)  FROM dim_product)  AS gold_products,
      (SELECT COALESCE(SUM(reject_count), 0) FROM quarantine_summary) AS quarantined_rows,
      (SELECT COALESCE(SUM(gross_amount), 0) FROM fact_orders) AS gross_revenue,
      (SELECT COUNT(*) FROM fact_orders WHERE is_late_arrival) AS late_arrivals
    `,
  );

  if (result.status !== "ok") {
    return NextResponse.json(result, { status: result.status === "pending" ? 202 : 502 });
  }

  const r = result.rows[0] ?? {};
  const num = (k: string) => Number(r[k] ?? 0);

  const goldRows = num("gold_fact_rows");
  const quarantined = num("quarantined_rows");

  return NextResponse.json({
    status: "ok",
    cached: result.cached,
    scannedBytes: result.scannedBytes,
    execMs: result.execMs,
    stages: {
      goldFactRows: goldRows,
      customers: num("gold_customers"),
      products: num("gold_products"),
      quarantinedRows: quarantined,
      lateArrivals: num("late_arrivals"),
      grossRevenue: num("gross_revenue"),
      // Share of records the platform rejected rather than silently accepted.
      quarantineRatePct:
        goldRows + quarantined > 0 ? (100 * quarantined) / (goldRows + quarantined) : 0,
    },
  });
}
