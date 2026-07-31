import { timingSafeEqual } from "node:crypto";
import { NextResponse, type NextRequest } from "next/server";

import { exchangeHandoffCode } from "@/server/amacrin";
import { sessionSecret } from "@/server/env";
import { isSecureRequest } from "@/server/request";
import {
  SESSION_COOKIE,
  SIGNIN_NONCE_COOKIE,
  clearNonceCookieOptions,
  createPlatformSessionValue,
  sessionCookieOptions,
} from "@/server/platform-session";

export const runtime = "nodejs";

/**
 * Redirect with a RELATIVE Location. In a node route handler an absolute URL is
 * built from the internal bind (0.0.0.0:3000), not the browser's address; a
 * relative Location lets the browser resolve it against the request URL. Mirrors
 * the self-host handoff route.
 */
function redirectTo(path: string): NextResponse {
  return new NextResponse(null, { status: 303, headers: { location: path } });
}

/** Length- and timing-safe equality for the CSRF nonce. */
function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

/**
 * Platform OAuth callback (issue #185, session-bearing BFF).
 *
 * The control plane bounces the browser here as `?n={nonce}&code={handoff}`
 * after Google sign-in. We verify the nonce against the cookie set at sign-in
 * (login-CSRF defence), exchange the one-time handoff code for a token pair
 * server-to-server, and seal that pair into the httpOnly session cookie. The
 * handoff code and the tokens never live in browser-accessible storage.
 */
export async function GET(req: NextRequest): Promise<NextResponse> {
  const params = req.nextUrl.searchParams;
  const nonceCookie = req.cookies.get(SIGNIN_NONCE_COOKIE)?.value;
  const nonce = params.get("n");

  const clearNonce = (res: NextResponse): NextResponse => {
    res.cookies.set(SIGNIN_NONCE_COOKIE, "", clearNonceCookieOptions(isSecureRequest(req)));
    return res;
  };

  // A brand-new email with the waitlist enabled: no account, no code.
  if (params.get("waitlisted") !== null) {
    return clearNonce(redirectTo("/sign-in?error=waitlisted"));
  }

  const code = params.get("code");
  if (code === null || nonce === null || nonceCookie === undefined || !safeEqual(nonce, nonceCookie)) {
    // Missing code, or a callback the browser didn't initiate (nonce mismatch).
    return clearNonce(redirectTo("/sign-in?error=invalid"));
  }

  let sessionValue: string;
  try {
    const tokens = await exchangeHandoffCode(code);
    sessionValue = await createPlatformSessionValue(
      { accessToken: tokens.accessToken, refreshToken: tokens.refreshToken },
      sessionSecret(),
    );
  } catch {
    return clearNonce(redirectTo("/sign-in?error=provider_error"));
  }

  const res = clearNonce(redirectTo("/"));
  res.cookies.set(SESSION_COOKIE, sessionValue, sessionCookieOptions(isSecureRequest(req)));
  return res;
}
