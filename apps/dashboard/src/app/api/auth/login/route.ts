import { timingSafeEqual } from "node:crypto";
import { NextResponse, type NextRequest } from "next/server";

import {
  dashboardPassword,
  dashboardUsername,
  jwtSecret,
  sessionSecret,
} from "@/server/env";
import { mintLocalAdminToken } from "@/server/mint-token";
import { isSameOrigin, isSecureRequest } from "@/server/request";
import {
  SESSION_COOKIE,
  createSessionValue,
  sessionCookieOptions,
} from "@/server/session";

export const runtime = "nodejs";

/** Length-safe, timing-safe string comparison. */
function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

/**
 * Self-host dashboard login. Verifies the dashboard credential, mints a
 * SUPERADMIN archive token, and sets it (wrapped in a signed httpOnly session
 * cookie) so subsequent archive calls proxy through with real auth.
 */
export async function POST(req: NextRequest): Promise<NextResponse> {
  if (!isSameOrigin(req)) {
    return NextResponse.json({ error: "invalid_origin" }, { status: 403 });
  }

  let body: { username?: unknown; password?: unknown };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "invalid_body" }, { status: 400 });
  }
  const username = typeof body.username === "string" ? body.username : "";
  const password = typeof body.password === "string" ? body.password : "";

  // Evaluate both to avoid short-circuiting on which field was wrong.
  const okUser = safeEqual(username, dashboardUsername());
  const okPass = safeEqual(password, dashboardPassword());
  if (!okUser || !okPass) {
    return NextResponse.json({ error: "invalid_credentials" }, { status: 401 });
  }

  const osaToken = mintLocalAdminToken(jwtSecret());
  const value = await createSessionValue(
    { sub: "admin@osa.local", osaToken },
    sessionSecret(),
  );

  const res = NextResponse.json({ ok: true });
  res.cookies.set(SESSION_COOKIE, value, sessionCookieOptions(isSecureRequest(req)));
  return res;
}
