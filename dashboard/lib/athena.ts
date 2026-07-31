/**
 * Server-only Athena client.
 *
 * Owner: Hemanjali Buchireddy
 *
 * `import "server-only"` is the enforcement mechanism, not a convention: if any
 * client component ever imports this module (directly or transitively), the
 * BUILD FAILS. That is a stronger guarantee than a code-review rule about where
 * AWS SDK imports are allowed.
 *
 * Athena is asynchronous. There is no "run this query and give me rows" call.
 * The sequence is:
 *
 *   StartQueryExecution  -> returns a QueryExecutionId immediately
 *   GetQueryExecution    -> poll until SUCCEEDED / FAILED / CANCELLED
 *   GetQueryResults      -> fetch rows
 *
 * Polling is bounded. If a query has not finished within the budget the route
 * returns a `pending` payload rather than holding the HTTP request open until a
 * gateway timeout — the UI renders a pending state instead of hanging.
 */

import "server-only";

import {
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";

const REGION = process.env.AWS_REGION ?? "us-east-1";
const WORKGROUP = process.env.ATHENA_WORKGROUP ?? "hb-dashboard-wg";
const DATABASE = process.env.ATHENA_DATABASE ?? "hb_lakehouse";

// Credentials come from the standard provider chain. On Vercel they arrive as
// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the encrypted env store — the
// hb-dashboard-reader user, which can only query one scan-capped workgroup and
// read the gold and bench prefixes. Deliberately NOT prefixed NEXT_PUBLIC_.
const athena = new AthenaClient({ region: REGION });

export type QueryOutcome<T> =
  | { status: "ok"; rows: T[]; scannedBytes: number; execMs: number; cached: boolean }
  | { status: "pending"; queryId: string; message: string }
  | { status: "error"; message: string };

type CacheEntry = { expires: number; value: QueryOutcome<Record<string, string>> };

/**
 * Server-side cache, mandatory rather than an optimisation.
 *
 * Athena bills $5 per TB scanned with a 10 MB minimum PER QUERY. Without this,
 * someone holding down refresh would issue one billable query per keypress.
 * 60 s is the floor set by the project's cost rules; 90 s is used here for a
 * little extra headroom.
 *
 * Note this is per-serverless-instance memory. Vercel may run several instances,
 * so the effective floor is (instances x 1 query / 90 s) — still bounded, and
 * the workgroup's 200 MB scan cutoff is the hard backstop underneath it.
 */
const CACHE_TTL_MS = 90_000;
const cache = new Map<string, CacheEntry>();

const MAX_POLL_MS = 8_000;
const POLL_INTERVAL_MS = 350;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function runQuery(
  cacheKey: string,
  sql: string,
): Promise<QueryOutcome<Record<string, string>>> {
  const hit = cache.get(cacheKey);
  if (hit && hit.expires > Date.now()) {
    return hit.value.status === "ok" ? { ...hit.value, cached: true } : hit.value;
  }

  let queryId: string;
  try {
    const started = await athena.send(
      new StartQueryExecutionCommand({
        QueryString: sql,
        QueryExecutionContext: { Database: DATABASE },
        WorkGroup: WORKGROUP,
      }),
    );
    queryId = started.QueryExecutionId!;
  } catch (err) {
    return { status: "error", message: `StartQueryExecution failed: ${(err as Error).message}` };
  }

  const deadline = Date.now() + MAX_POLL_MS;
  let state = "QUEUED";
  let scannedBytes = 0;
  let execMs = 0;
  let failureReason = "";

  while (Date.now() < deadline) {
    const info = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
    const exec = info.QueryExecution!;
    state = exec.Status?.State ?? "UNKNOWN";

    if (state === "SUCCEEDED") {
      scannedBytes = exec.Statistics?.DataScannedInBytes ?? 0;
      execMs = exec.Statistics?.EngineExecutionTimeInMillis ?? 0;
      break;
    }
    if (state === "FAILED" || state === "CANCELLED") {
      failureReason = exec.Status?.StateChangeReason ?? state;
      break;
    }
    await sleep(POLL_INTERVAL_MS);
  }

  if (state === "FAILED" || state === "CANCELLED") {
    return { status: "error", message: failureReason };
  }

  if (state !== "SUCCEEDED") {
    // Still running. Return control to the browser rather than holding the
    // request open — the client polls this route again shortly.
    return {
      status: "pending",
      queryId,
      message: `Query still ${state.toLowerCase()} after ${MAX_POLL_MS / 1000}s`,
    };
  }

  const results = await athena.send(new GetQueryResultsCommand({ QueryExecutionId: queryId }));
  const raw = results.ResultSet?.Rows ?? [];
  if (raw.length === 0) {
    const empty: QueryOutcome<Record<string, string>> = {
      status: "ok",
      rows: [],
      scannedBytes,
      execMs,
      cached: false,
    };
    cache.set(cacheKey, { expires: Date.now() + CACHE_TTL_MS, value: empty });
    return empty;
  }

  // Athena returns the header as the first row.
  const header = (raw[0].Data ?? []).map((c) => c.VarCharValue ?? "");
  const rows = raw.slice(1).map((r) => {
    const obj: Record<string, string> = {};
    (r.Data ?? []).forEach((cell, i) => {
      obj[header[i] ?? `col${i}`] = cell.VarCharValue ?? "";
    });
    return obj;
  });

  const value: QueryOutcome<Record<string, string>> = {
    status: "ok",
    rows,
    scannedBytes,
    execMs,
    cached: false,
  };
  cache.set(cacheKey, { expires: Date.now() + CACHE_TTL_MS, value });
  return value;
}

export const config = { REGION, WORKGROUP, DATABASE, CACHE_TTL_MS };
