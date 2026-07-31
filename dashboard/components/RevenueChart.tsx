"use client";

/**
 * Daily revenue from the gold star schema.
 *
 * Owner: Hemanjali Buchireddy
 *
 * NO AWS SDK IMPORTS. This is a client component; it receives already-fetched
 * JSON as props. All AWS access happens in /app/api/revenue/route.ts on the
 * server. lib/athena.ts carries `import "server-only"`, so if this file ever
 * reached for it the build would fail rather than leak a credential.
 *
 * Chart decisions, per the data-viz method:
 *   - Form: change over time, one measure -> line.
 *   - One series, so NO legend box; the title names it.
 *   - 2px stroke, 8px hover marker, recessive hairline grid, muted axes.
 *   - Values and labels wear text tokens, never the series color.
 *   - Crosshair + tooltip by default; an HTML chart is interactive.
 *   - A table view exists, so identity is never carried by color alone.
 */

import { useEffect, useMemo, useRef, useState } from "react";

export type RevenuePoint = {
  day: string;
  dayName: string;
  isWeekend: boolean;
  orderLines: number;
  revenue: number;
  avgOrderValue: number;
};

const PAD = { top: 16, right: 18, bottom: 30, left: 58 };
const HEIGHT = 260;

function money(n: number) {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}k`;
  return `$${n.toFixed(0)}`;
}

export default function RevenueChart({ series }: { series: RevenuePoint[] }) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(720);
  const [hover, setHover] = useState<number | null>(null);
  const [showTable, setShowTable] = useState(false);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const geom = useMemo(() => {
    const innerW = Math.max(120, width - PAD.left - PAD.right);
    const innerH = HEIGHT - PAD.top - PAD.bottom;
    const max = Math.max(1, ...series.map((d) => d.revenue));
    // Round the top of the scale up so gridlines land on readable numbers.
    const niceMax = Math.pow(10, Math.floor(Math.log10(max))) *
      Math.ceil(max / Math.pow(10, Math.floor(Math.log10(max))));

    const x = (i: number) =>
      PAD.left + (series.length <= 1 ? innerW / 2 : (i * innerW) / (series.length - 1));
    const y = (v: number) => PAD.top + innerH - (v / niceMax) * innerH;

    const path = series.map((d, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(d.revenue)}`).join(" ");
    const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => ({ v: niceMax * f, y: y(niceMax * f) }));

    return { innerW, innerH, niceMax, x, y, path, ticks };
  }, [series, width]);

  if (series.length === 0) {
    return (
      <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
        No revenue rows yet — run the pipeline to populate <code>fact_orders</code>.
      </p>
    );
  }

  const active = hover !== null ? series[hover] : null;

  return (
    <div>
      <div className="flex items-baseline justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold" style={{ color: "var(--text-primary)" }}>
            Daily revenue
          </h2>
          <p className="text-xs" style={{ color: "var(--text-secondary)" }}>
            <code>fact_orders</code> joined to <code>dim_date</code> on the surrogate{" "}
            <code>date_key</code>
          </p>
        </div>
        <button
          onClick={() => setShowTable((v) => !v)}
          className="rounded px-2 py-1 text-xs"
          style={{ color: "var(--text-secondary)", border: "1px solid var(--hairline)" }}
        >
          {showTable ? "Show chart" : "Show table"}
        </button>
      </div>

      {showTable ? (
        <div className="mt-3 max-h-64 overflow-auto">
          <table className="w-full text-left text-xs tnum">
            <thead style={{ color: "var(--text-muted)" }}>
              <tr>
                <th className="py-1 pr-3 font-medium">Date</th>
                <th className="py-1 pr-3 font-medium">Day</th>
                <th className="py-1 pr-3 text-right font-medium">Order lines</th>
                <th className="py-1 text-right font-medium">Revenue</th>
              </tr>
            </thead>
            <tbody style={{ color: "var(--text-secondary)" }}>
              {series.map((d) => (
                <tr key={d.day} style={{ borderTop: "1px solid var(--gridline)" }}>
                  <td className="py-1 pr-3">{d.day}</td>
                  <td className="py-1 pr-3">{d.dayName}</td>
                  <td className="py-1 pr-3 text-right">{d.orderLines.toLocaleString()}</td>
                  <td className="py-1 text-right">${d.revenue.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div ref={wrapRef} className="relative mt-3">
          <svg
            width="100%"
            height={HEIGHT}
            role="img"
            aria-label={`Daily revenue across ${series.length} days`}
            onMouseLeave={() => setHover(null)}
            onMouseMove={(e) => {
              const rect = e.currentTarget.getBoundingClientRect();
              const px = e.clientX - rect.left;
              const ratio = (px - PAD.left) / Math.max(1, geom.innerW);
              const idx = Math.round(ratio * (series.length - 1));
              setHover(Math.max(0, Math.min(series.length - 1, idx)));
            }}
          >
            {/* Recessive hairline grid — present, never competing with the data. */}
            {geom.ticks.map((t) => (
              <g key={t.v}>
                <line
                  x1={PAD.left}
                  x2={width - PAD.right}
                  y1={t.y}
                  y2={t.y}
                  stroke="var(--gridline)"
                  strokeWidth={1}
                />
                <text
                  x={PAD.left - 8}
                  y={t.y + 3}
                  textAnchor="end"
                  fontSize={10}
                  className="tnum"
                  fill="var(--text-muted)"
                >
                  {money(t.v)}
                </text>
              </g>
            ))}

            <line
              x1={PAD.left}
              x2={width - PAD.right}
              y1={PAD.top + geom.innerH}
              y2={PAD.top + geom.innerH}
              stroke="var(--baseline)"
              strokeWidth={1}
            />

            {/* 2px stroke, per the mark spec. */}
            <path
              d={geom.path}
              fill="none"
              stroke="var(--series-1)"
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
            />

            {/* Crosshair + 8px marker, with a 2px surface ring so the mark stays
                legible wherever it lands on the line. */}
            {active && hover !== null && (
              <g>
                <line
                  x1={geom.x(hover)}
                  x2={geom.x(hover)}
                  y1={PAD.top}
                  y2={PAD.top + geom.innerH}
                  stroke="var(--baseline)"
                  strokeWidth={1}
                  strokeDasharray="3 3"
                />
                <circle
                  cx={geom.x(hover)}
                  cy={geom.y(active.revenue)}
                  r={5}
                  fill="var(--series-1)"
                  stroke="var(--surface-1)"
                  strokeWidth={2}
                />
              </g>
            )}

            {/* Selective direct labels: first and last only, never every point. */}
            <text
              x={PAD.left}
              y={HEIGHT - 10}
              fontSize={10}
              fill="var(--text-muted)"
              className="tnum"
            >
              {series[0].day}
            </text>
            <text
              x={width - PAD.right}
              y={HEIGHT - 10}
              fontSize={10}
              textAnchor="end"
              fill="var(--text-muted)"
              className="tnum"
            >
              {series[series.length - 1].day}
            </text>
          </svg>

          {active && hover !== null && (
            <div
              className="pointer-events-none absolute rounded px-2 py-1 text-xs shadow-sm"
              style={{
                left: Math.min(Math.max(geom.x(hover) - 60, 0), Math.max(0, width - 150)),
                top: 4,
                background: "var(--surface-1)",
                border: "1px solid var(--hairline)",
                color: "var(--text-primary)",
              }}
            >
              <div className="font-medium">
                {active.day} · {active.dayName}
              </div>
              <div className="tnum" style={{ color: "var(--text-secondary)" }}>
                ${active.revenue.toLocaleString()} · {active.orderLines.toLocaleString()} lines
              </div>
              <div className="tnum" style={{ color: "var(--text-secondary)" }}>
                AOV ${active.avgOrderValue.toFixed(2)}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
