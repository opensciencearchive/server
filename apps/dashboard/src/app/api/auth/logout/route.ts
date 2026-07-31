import { NextResponse, type NextRequest } from "next/server";

import { isPlatformFromEnv } from "@/api/config";
import { revokeSession } from "@/server/amacrin";
import { sessionSecret } from "@/server/env";
import { readPlatformSession } from "@/server/platform-session";
import { isSameOrigin } from "@/server/request";
import { SESSION_COOKIE } from "@/server/session";

export const runtime = "nodejs";

/**
 * Clear the dashboard session cookie. Idempotent.
 *
 * Self-host: just drop the cookie (the sealed token is a local mint). Platform
 * (#185): also revoke the session server-side at the control plane using the
 * sealed refresh token, so the credential is dead even before the cookie's TTL.
 */
export async function POST(req: NextRequest): Promise<NextResponse> {
  if (!isSameOrigin(req)) {
    return NextResponse.json({ error: "invalid_origin" }, { status: 403 });
  }

  if (isPlatformFromEnv()) {
    const session = await readPlatformSession(
      req.cookies.get(SESSION_COOKIE)?.value,
      sessionSecret(),
    );
    if (session !== null) {
      // Best-effort revocation; logout succeeds regardless (the user asked to
      // leave, and clearing the cookie ends the browser session either way).
      try {
        await revokeSession(session.refreshToken);
      } catch {
        // Ignore — cookie teardown below still signs the browser out.
      }
    }
  }

  const res = NextResponse.json({ ok: true });
  res.cookies.set(SESSION_COOKIE, "", {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    maxAge: 0,
  });
  return res;
}
