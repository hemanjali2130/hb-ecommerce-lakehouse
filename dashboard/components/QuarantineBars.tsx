"use client";

/**
 * Quarantine counts broken down by rejection reason.
 *
 * Owner: Hemanjali Buchireddy
 *
 * Form: ranked magnitude across a handful of named categories -> horizontal
 * bars. Horizontal because the reason labels are long words, and horizontal bars
 * give labels a full line instead of forcing rotated axis text.
 *
 * ONE hue, not a categorical palette: the bars encode magnitude of the same
 * quantity, so giving each reason its own color would imply an identity
 * distinction that does not exist. Rank carries the comparison; length carries
 * the value.
 *
 * Deliberately NOT the "critical" status color. Status colors are reserved for
 * state and must ship with an icon + label — and quarantining a malformed record
 * is the system working correctly, not an incident.
 *
 * No AWS SDK imports. Props arrive from /app/api/quarantine/route.ts.
 */

import { useState } from "react";

export type QuarantineReason = {
  reason: string;
  label: string;
  count: number;
  sharePct: number;
  lastSeenAt: string | null;
};

export default function QuarantineBars({
  reasons,
  total,
}: {
  reasons: QuarantineReason[];
  total: number;
}) {
  const [hover, setHover] = useState<string | null>(null);

  if (reasons.length === 0) {
    return (
      <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
        Nothing quarantined yet.
      </p>
    );
  }

  const max = Math.max(...reasons.map((r) => r.count));

  return (
    <div>
      <div className="flex items-baseline justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold" style={{ color: "var(--text-primary)" }}>
            Quarantine by rejection reason
          </h2>
          <p className="text-xs" style={{ color: "var(--text-secondary)" }}>
            {total.toLocaleString()} records rejected — every one persisted with its reason
            before being dropped
          </p>
        </div>
      </div>

      <ul className="mt-4 space-y-3">
        {reasons.map((r) => (
          <li
            key={r.reason}
            onMouseEnter={() => setHover(r.reason)}
            onMouseLeave={() => setHover(null)}
          >
            <div className="flex items-baseline justify-between gap-3 text-xs">
              {/* Label wears an ink token, never the series color. */}
              <span style={{ color: "var(--text-primary)" }}>{r.label}</span>
              <span className="tnum shrink-0" style={{ color: "var(--text-secondary)" }}>
                {r.count.toLocaleString()}
                <span style={{ color: "var(--text-muted)" }}> · {r.sharePct.toFixed(1)}%</span>
              </span>
            </div>

            {/* Track + fill. 4px rounded data-end, anchored flat to the baseline. */}
            <div
              className="mt-1 h-2 w-full overflow-hidden rounded-sm"
              style={{ background: "var(--gridline)" }}
            >
              <div
                className="h-full transition-[width] duration-300"
                style={{
                  width: `${Math.max(2, (100 * r.count) / max)}%`,
                  background: "var(--series-2)",
                  borderTopRightRadius: 4,
                  borderBottomRightRadius: 4,
                  opacity: hover === null || hover === r.reason ? 1 : 0.55,
                }}
              />
            </div>

            <p className="mt-1 font-mono text-[10px]" style={{ color: "var(--text-muted)" }}>
              {r.reason}
            </p>
          </li>
        ))}
      </ul>
    </div>
  );
}
