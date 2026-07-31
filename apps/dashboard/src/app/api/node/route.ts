import { NextResponse, type NextRequest } from "next/server";

import { osaApiUrl, sessionSecret } from "@/server/env";
import { SESSION_COOKIE, readSession } from "@/server/session";

export const runtime = "nodejs";

/**
 * BFF: the self-hosted node's overview for the Overview hero. Assembles root
 * discovery (`GET /`), readiness (`GET /api/v1/ready`), and stats
 * (`GET /api/v1/stats`). Each is fetched independently (allSettled) so a
 * missing/unavailable piece degrades to a fallback instead of failing the
 * whole page; only a failed discovery (no node identity) is a hard error.
 */
async function json(res: PromiseSettledResult<Response>): Promise<unknown> {
  if (res.status !== "fulfilled" || !res.value.ok) return null;
  try {
    return await res.value.json();
  } catch {
    return null;
  }
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

export async function GET(req: NextRequest): Promise<NextResponse> {
  const session = await readSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  const base = osaApiUrl();
  const auth = { authorization: `Bearer ${session.osaToken}` };
  const [discRes, readyRes, statsRes] = await Promise.allSettled([
    fetch(`${base}/`, { headers: { accept: "application/json" } }),
    fetch(`${base}/api/v1/ready`, { headers: { accept: "application/json" } }),
    fetch(`${base}/api/v1/stats`, { headers: { ...auth, accept: "application/json" } }),
  ]);

  const discovery = await json(discRes);
  if (discovery === null) {
    return NextResponse.json({ error: "unreachable" }, { status: 502 });
  }
  const node = record(record(discovery).node);
  const ready = record(await json(readyRes));
  const stats = record(await json(statsRes));

  const schemas = record(discovery).schemas;
  return NextResponse.json({
    name: node.name ?? "",
    domain: node.domain ?? "",
    description: node.description ?? "",
    osaVersion: node.osa_version ?? "",
    status: ready.status === "ready" || ready.status === "degraded"
      ? ready.status
      : "unknown",
    records: typeof stats.records === "number" ? stats.records : null,
    schemas: Array.isArray(schemas) ? schemas.length : 0,
  });
}
