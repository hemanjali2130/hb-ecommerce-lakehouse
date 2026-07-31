/**
 * Last pipeline run: status and duration, straight from Step Functions.
 *
 * Owner: Hemanjali Buchireddy
 *
 * Step Functions Standard retains 90 days of execution history and exposes it
 * through the API, which is exactly why this project chose Standard over
 * Express: no extra database is needed to answer "did the last run work?".
 *
 * This route does not touch Athena, so it costs nothing per call. It still gets
 * a short server-side cache to avoid hammering the SFN API on refresh.
 */

import { NextResponse } from "next/server";

import "server-only";

import {
  DescribeExecutionCommand,
  ListExecutionsCommand,
  SFNClient,
} from "@aws-sdk/client-sfn";

export const dynamic = "force-dynamic";

const sfn = new SFNClient({ region: process.env.AWS_REGION ?? "us-east-1" });
const STATE_MACHINE_ARN = process.env.STATE_MACHINE_ARN ?? "";

const CACHE_TTL_MS = 60_000;
let cache: { expires: number; body: unknown } | null = null;

export async function GET() {
  if (!STATE_MACHINE_ARN) {
    return NextResponse.json(
      { status: "error", message: "STATE_MACHINE_ARN is not configured" },
      { status: 500 },
    );
  }

  if (cache && cache.expires > Date.now()) {
    return NextResponse.json({ ...(cache.body as object), cached: true });
  }

  try {
    const list = await sfn.send(
      new ListExecutionsCommand({ stateMachineArn: STATE_MACHINE_ARN, maxResults: 5 }),
    );

    const executions = await Promise.all(
      (list.executions ?? []).map(async (e) => {
        const detail = await sfn.send(new DescribeExecutionCommand({ executionArn: e.executionArn! }));
        const start = detail.startDate ? new Date(detail.startDate).getTime() : null;
        const stop = detail.stopDate ? new Date(detail.stopDate).getTime() : null;
        return {
          name: detail.name,
          status: detail.status,
          startedAt: detail.startDate ?? null,
          stoppedAt: detail.stopDate ?? null,
          // null while still running, rather than a misleading zero.
          durationSeconds: start && stop ? Math.round((stop - start) / 1000) : null,
        };
      }),
    );

    const body = {
      status: "ok",
      latest: executions[0] ?? null,
      recent: executions,
      cached: false,
    };
    cache = { expires: Date.now() + CACHE_TTL_MS, body };
    return NextResponse.json(body);
  } catch (err) {
    return NextResponse.json(
      { status: "error", message: (err as Error).message },
      { status: 502 },
    );
  }
}
