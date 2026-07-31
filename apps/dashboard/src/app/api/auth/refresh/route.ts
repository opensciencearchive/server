import { NextResponse, type NextRequest } from "next/server";

import { sessionSecret } from "@/server/env";
import { coalesceRefresh } from "@/server/refresh-coalesce";
import { isSameOrigin, isSecureRequest } from "@/server/request";
import {
  SESSION_COOKIE,
  createPlatformSessionValue,
  readPlatformSession,
  sessionCookieOptions,
} from "@/server/platform-session";

export const runtime = "nodejs";

/**
 * Force a platform session refresh (issue #185).
 *
 * The access token embeds the user's org permissions, so after an action that
 * changes them (creating an organisation) the client asks the BFF to rotate the
 * pair immediately — otherwise the new org 404s until the old token expires.
 * Server-side port of the retired client `SessionRefresher.forceRefresh()`.
 */
export async function POST(req: NextRequest): Promise<NextResponse> {
  if (!isSameOrigin(req)) {
    return NextResponse.json({ error: "invalid_origin" }, { status: 403 });
  }

  const session = await readPlatformSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  try {
    const tokens = await coalesceRefresh(session.refreshToken);
    const value = await createPlatformSessionValue(
      { accessToken: tokens.accessToken, refreshToken: tokens.refreshToken },
      sessionSecret(),
    );
    const res = NextResponse.json({ ok: true });
    res.cookies.set(SESSION_COOKIE, value, sessionCookieOptions(isSecureRequest(req)));
    return res;
  } catch {
    return NextResponse.json({ error: "refresh_failed" }, { status: 401 });
  }
}
