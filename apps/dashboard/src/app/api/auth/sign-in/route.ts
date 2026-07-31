import { randomBytes } from "node:crypto";
import { NextResponse, type NextRequest } from "next/server";

import { amacrinPublicUrl } from "@/server/env";
import { isSecureRequest, requestOrigin } from "@/server/request";
import { SIGNIN_NONCE_COOKIE, nonceCookieOptions } from "@/server/platform-session";

export const runtime = "nodejs";

/**
 * Platform sign-in initiation (issue #185, session-bearing BFF).
 *
 * Sets a single-use CSRF nonce cookie and 302s the browser to the control
 * plane's Google OAuth entry point, asking it to bounce back to this BFF's
 * callback with the nonce round-tripped through `redirect_uri`. The browser
 * only ever touches the control plane's *public* origin here; the token
 * exchange (callback) and all data reads happen server-side over the in-cluster
 * `AMACRIN_API_URL`.
 */
export async function GET(req: NextRequest): Promise<NextResponse> {
  const nonce = randomBytes(16).toString("hex");

  // Round-trip the nonce through the control plane: the callback lands at
  // `{origin}/api/auth/callback?n={nonce}` (the cloud appends `&code=…`), and we
  // reject the handoff code unless `n` matches this cookie.
  const callbackUrl = new URL("/api/auth/callback", requestOrigin(req));
  callbackUrl.searchParams.set("n", nonce);

  const loginUrl = new URL("/api/v1/auth/login", amacrinPublicUrl());
  loginUrl.searchParams.set("redirect_uri", callbackUrl.toString());
  loginUrl.searchParams.set("provider", "google");

  const res = NextResponse.redirect(loginUrl, { status: 302 });
  res.cookies.set(SIGNIN_NONCE_COOKIE, nonce, nonceCookieOptions(isSecureRequest(req)));
  return res;
}
