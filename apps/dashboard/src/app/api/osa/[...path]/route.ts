import { NextResponse, type NextRequest } from "next/server";

import { osaApiUrl, sessionSecret } from "@/server/env";
import { SESSION_COOKIE, readSession } from "@/server/session";

export const runtime = "nodejs";

/**
 * BFF read proxy for the local archive (issue #173).
 *
 * The browser never holds the archive token; it calls same-origin `/api/osa/*`
 * and this handler attaches the SUPERADMIN bearer from the session cookie and
 * forwards to the archive. Restricted to **GET** on a fixed **read** allowlist
 * so the long-lived superadmin token can't be replayed against write endpoints.
 */
const ALLOWED_ROOTS = new Set(["stats", "data", "schemas", "hooks"]);

export async function GET(
  req: NextRequest,
  ctx: { params: Promise<{ path: string[] }> },
): Promise<NextResponse> {
  const session = await readSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  const { path } = await ctx.params;
  if (path.length === 0 || !ALLOWED_ROOTS.has(path[0]!)) {
    return NextResponse.json({ error: "not_allowed" }, { status: 403 });
  }

  const target = `${osaApiUrl()}/api/v1/${path.join("/")}${req.nextUrl.search}`;
  const upstream = await fetch(target, {
    headers: {
      authorization: `Bearer ${session.osaToken}`,
      accept: req.headers.get("accept") ?? "application/json",
    },
  });

  const headers = new Headers();
  const contentType = upstream.headers.get("content-type");
  if (contentType !== null) headers.set("content-type", contentType);
  return new NextResponse(upstream.body, {
    status: upstream.status,
    headers,
  });
}
