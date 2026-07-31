import { NextResponse, type NextRequest } from "next/server";

import { osaApiUrl, sessionSecret } from "@/server/env";
import { SESSION_COOKIE, readSession } from "@/server/session";

export const runtime = "nodejs";

/**
 * BFF: the node's sign-in configuration (ORCID provider + client id + admins).
 *
 * A bespoke route rather than a generic-proxy root so the whole `/api/v1/auth/*`
 * surface (login, callback, me) is NOT reachable through the proxy — only the
 * ADMIN-gated `/api/v1/auth/config` read, with the session's SUPERADMIN bearer
 * attached server-side.
 */
export async function GET(req: NextRequest): Promise<NextResponse> {
  const session = await readSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  const upstream = await fetch(`${osaApiUrl()}/api/v1/auth/config`, {
    headers: {
      authorization: `Bearer ${session.osaToken}`,
      accept: "application/json",
    },
  });

  if (!upstream.ok) {
    return NextResponse.json({ error: "unreachable" }, { status: 502 });
  }

  return NextResponse.json(await upstream.json());
}
