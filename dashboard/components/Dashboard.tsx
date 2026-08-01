"use client";

/**
 * Dashboard shell.
 *
 * Owner: Hemanjali Buchireddy
 *
 * Fetches from this app's OWN route handlers — never from AWS. There is no AWS
 * SDK import anywhere in this file or anything it imports, and no NEXT_PUBLIC_*
 * credential exists to import.
 *
 * Athena is asynchronous, so a route can legitimately answer 202 with
 * `{status:"pending"}` when a query has not finished inside the server's polling
 * budget. That is rendered as an explicit pending state and retried, rather than
 * holding the request open until a gateway timeout.
 */

import { useCallback, useEffect, useState } from "react";

import QuarantineBars, { type QuarantineReason } from "./QuarantineBars";
import RevenueChart, { type RevenuePoint } from "./RevenueChart";

type Fetched<T> =
  | { state: "loading" }
  | { state: "pending"; message: string }
  | { state: "error"; message: string }
  | { state: "ok"; data: T };

function useEndpoint<T>(url: string, refreshMs = 90_000): Fetched<T> {
  const [result, setResult] = useState<Fetched<T>>({ state: "loading" });

  const load = useCallback(async () => {
    try {
      const res = await fetch(url, { cache: "no-store" });
      const body = await res.json();
      if (res.status === 202 || body.status === "pending") {
        setResult({ state: "pending", message: body.message ?? "Query running" });
        // Athena queries that exceed the server's polling budget usually finish
        // within a few seconds; retry rather than leaving the panel blank.
        setTimeout(load, 4000);
        return;
      }
      if (!res.ok || body.status === "error") {
        setResult({ state: "error", message: body.message ?? `HTTP ${res.status}` });
        return;
      }
      setResult({ state: "ok", data: body as T });
    } catch (err) {
      setResult({ state: "error", message: (err as Error).message });
    }
  }, [url]);

  useEffect(() => {
    load();
    const id = setInterval(load, refreshMs);
    return () => clearInterval(id);
  }, [load, refreshMs]);

  return result;
}

function Panel({
  title,
  children,
  className = "",
}: {
  title?: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={`card p-4 ${className}`}>
      {title && (
        <h2 className="mb-3 text-base font-semibold" style={{ color: "var(--text-primary)" }}>
          {title}
        </h2>
      )}
      {children}
    </section>
  );
}

function Status({ s }: { s: Fetched<unknown> }) {
  if (s.state === "loading")
    return (
      <p className="text-sm" style={{ color: "var(--text-muted)" }}>
        Loading…
      </p>
    );
  if (s.state === "pending")
    return (
      <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
        <span
          className="mr-2 inline-block h-2 w-2 animate-pulse rounded-full align-middle"
          style={{ background: "var(--status-warning)" }}
        />
        {s.message} — Athena is still running this query.
      </p>
    );
  if (s.state === "error")
    return (
      <p className="text-sm" style={{ color: "var(--status-critical)" }}>
        {s.message}
      </p>
    );
  return null;
}

function StatTile({
  label,
  value,
  sub,
  tone,
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "good" | "warning" | "critical";
}) {
  const toneColor =
    tone === "good"
      ? "var(--status-good)"
      : tone === "warning"
        ? "var(--status-warning)"
        : tone === "critical"
          ? "var(--status-critical)"
          : "var(--text-primary)";

  return (
    <div className="card p-4">
      <p className="text-xs" style={{ color: "var(--text-secondary)" }}>
        {label}
      </p>
      <p className="mt-1 text-2xl font-semibold" style={{ color: toneColor }}>
        {value}
      </p>
      {sub && (
        <p className="mt-0.5 text-xs" style={{ color: "var(--text-muted)" }}>
          {sub}
        </p>
      )}
    </div>
  );
}

type Metrics = {
  stages: {
    goldFactRows: number;
    customers: number;
    products: number;
    quarantinedRows: number;
    lateArrivals: number;
    grossRevenue: number;
    totalValidEvents: number;
    totalGeneratedEvents: number;
    quarantineRatePct: number;
  };
};
type Freshness = {
  lagSeconds: number;
  health: "fresh" | "lagging" | "stale";
  maxEventTs: string | null;
  distinctDays: number;
};
type Quarantine = { total: number; reasons: QuarantineReason[] };
type Pipeline = {
  latest: {
    name: string;
    status: string;
    durationSeconds: number | null;
    startedAt: string | null;
  } | null;
  recent: { name: string; status: string; durationSeconds: number | null }[];
};
type Revenue = { series: RevenuePoint[]; totals: { revenue: number; orderLines: number } };

function humanLag(sec: number) {
  if (sec < 90) return `${Math.round(sec)}s`;
  if (sec < 5400) return `${Math.round(sec / 60)}m`;
  if (sec < 172_800) return `${(sec / 3600).toFixed(1)}h`;
  return `${(sec / 86_400).toFixed(1)}d`;
}

export default function Dashboard() {
  const metrics = useEndpoint<Metrics>("/api/metrics");
  const freshness = useEndpoint<Freshness>("/api/freshness");
  const quarantine = useEndpoint<Quarantine>("/api/quarantine");
  const pipeline = useEndpoint<Pipeline>("/api/pipeline", 60_000);
  const revenue = useEndpoint<Revenue>("/api/revenue");

  return (
    <main className="mx-auto max-w-6xl px-5 py-8">
      <header className="mb-6">
        <h1 className="text-xl font-semibold" style={{ color: "var(--text-primary)" }}>
          hb-ecommerce-lakehouse
        </h1>
        <p className="mt-1 text-sm" style={{ color: "var(--text-secondary)" }}>
          Serverless AWS lakehouse — Firehose ingestion with in-flight validation, Glue
          medallion pipeline, Kimball star schema on Athena. Built by Hemanjali Buchireddy.
        </p>
        <p className="mt-1 text-xs" style={{ color: "var(--text-muted)" }}>
          Every AWS call runs server-side. Results cached ≥60s and the Athena workgroup
          enforces a 200&nbsp;MB per-query scan cap, so refreshing cannot run up a bill.
        </p>
      </header>

      {/* Stat tiles */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {metrics.state === "ok" ? (
          <>
            <StatTile
              label="Order lines in gold"
              value={metrics.data.stages.goldFactRows.toLocaleString()}
              sub={`${metrics.data.stages.totalValidEvents.toLocaleString()} valid events · ${metrics.data.stages.customers.toLocaleString()} customers · ${metrics.data.stages.products.toLocaleString()} products`}
            />
            <StatTile
              label="Gross revenue"
              value={`$${Math.round(metrics.data.stages.grossRevenue).toLocaleString()}`}
              sub="SUM(gross_amount) from fact_orders"
            />
            <StatTile
              label="Quarantined"
              value={metrics.data.stages.quarantinedRows.toLocaleString()}
              sub={`${metrics.data.stages.quarantineRatePct.toFixed(2)}% of ${metrics.data.stages.totalGeneratedEvents.toLocaleString()} generated events`}
              tone="warning"
            />
            <StatTile
              label="Late arrivals"
              value={metrics.data.stages.lateArrivals.toLocaleString()}
              sub="backdated >1h, partitioned by true event date"
            />
          </>
        ) : (
          <div className="col-span-2 md:col-span-4">
            <Panel>
              <Status s={metrics} />
            </Panel>
          </div>
        )}
      </div>

      <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
        {/* Freshness */}
        <Panel title="Data freshness">
          {freshness.state === "ok" ? (
            <>
              <p
                className="text-3xl font-semibold"
                style={{
                  color:
                    freshness.data.health === "fresh"
                      ? "var(--status-good)"
                      : freshness.data.health === "lagging"
                        ? "var(--status-warning)"
                        : "var(--status-critical)",
                }}
              >
                {humanLag(freshness.data.lagSeconds)}
              </p>
              <p className="mt-1 text-xs" style={{ color: "var(--text-secondary)" }}>
                now − max(event_timestamp) in gold ·{" "}
                <strong style={{ color: "var(--text-primary)" }}>{freshness.data.health}</strong>
              </p>
              <p className="mt-1 text-xs tnum" style={{ color: "var(--text-muted)" }}>
                latest event {freshness.data.maxEventTs ?? "—"} · {freshness.data.distinctDays}{" "}
                distinct days
              </p>
            </>
          ) : (
            <Status s={freshness} />
          )}
        </Panel>

        {/* Pipeline */}
        <Panel title="Last pipeline run">
          {pipeline.state === "ok" && pipeline.data.latest ? (
            <>
              <p
                className="text-3xl font-semibold"
                style={{
                  color:
                    pipeline.data.latest.status === "SUCCEEDED"
                      ? "var(--status-good)"
                      : pipeline.data.latest.status === "RUNNING"
                        ? "var(--status-warning)"
                        : "var(--status-critical)",
                }}
              >
                {pipeline.data.latest.status}
              </p>
              <p className="mt-1 text-xs tnum" style={{ color: "var(--text-secondary)" }}>
                {pipeline.data.latest.durationSeconds !== null
                  ? `${pipeline.data.latest.durationSeconds}s duration`
                  : "still running"}{" "}
                · started {pipeline.data.latest.startedAt ?? "—"}
              </p>
              <ul className="mt-3 space-y-1">
                {pipeline.data.recent.slice(1, 4).map((e) => (
                  <li
                    key={e.name}
                    className="flex justify-between text-xs tnum"
                    style={{ color: "var(--text-muted)" }}
                  >
                    <span>{e.status}</span>
                    <span>{e.durationSeconds !== null ? `${e.durationSeconds}s` : "—"}</span>
                  </li>
                ))}
              </ul>
            </>
          ) : pipeline.state === "ok" ? (
            <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
              No executions yet.
            </p>
          ) : (
            <Status s={pipeline} />
          )}
        </Panel>
      </div>

      {/* Chart */}
      <div className="mt-3">
        <Panel>
          {revenue.state === "ok" ? (
            <RevenueChart series={revenue.data.series} />
          ) : (
            <Status s={revenue} />
          )}
        </Panel>
      </div>

      {/* Quarantine */}
      <div className="mt-3">
        <Panel>
          {quarantine.state === "ok" ? (
            <QuarantineBars reasons={quarantine.data.reasons} total={quarantine.data.total} />
          ) : (
            <Status s={quarantine} />
          )}
        </Panel>
      </div>

      <footer className="mt-8 text-xs" style={{ color: "var(--text-muted)" }}>
        Read-only IAM user scoped to one Athena workgroup, the gold prefix, and Step Functions
        run history. No credentials reach the browser.
      </footer>
    </main>
  );
}
