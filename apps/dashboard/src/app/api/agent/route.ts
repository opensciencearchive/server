import { NextResponse, type NextRequest } from "next/server";

import { osaApiUrl, sessionSecret } from "@/server/env";
import { SESSION_COOKIE, readSession } from "@/server/session";

export const runtime = "nodejs";

/**
 * BFF fetch for the archive's agent-grounding surface (issue #173):
 * `GET /SKILL.md` (markdown) + `GET /` (root discovery JSON). Both are public
 * root-level endpoints (not under `/api/v1`), so they can't ride the `/api/osa`
 * proxy — this route fetches them for the dashboard's Agents page.
 */
export async function GET(req: NextRequest): Promise<NextResponse> {
  const session = await readSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  const base = osaApiUrl();
  const [skillRes, discoveryRes] = await Promise.all([
    fetch(`${base}/SKILL.md`, { headers: { accept: "text/markdown" } }),
    fetch(`${base}/`, { headers: { accept: "application/json" } }),
  ]);

  if (!skillRes.ok || !discoveryRes.ok) {
    return NextResponse.json(
      { error: "upstream_error" },
      { status: 502 },
    );
  }

  return NextResponse.json({
    skill: await skillRes.text(),
    discovery: await discoveryRes.json(),
  });
}
