import { NextResponse, type NextRequest } from "next/server";

import { jwtSecret, sessionSecret } from "@/server/env";
import { mintLocalAdminToken } from "@/server/mint-token";
import { isSecureRequest } from "@/server/request";
import {
  SESSION_COOKIE,
  createSessionValue,
  sessionCookieOptions,
  verifyHandoffProof,
} from "@/server/session";

export const runtime = "nodejs";

/**
 * CLI passwordless handoff (issue #173). `osa dashboard` mints a short-lived
 * proof signed with the shared SESSION_SECRET and points the browser here. Only
 * a holder of that secret can forge a valid proof, so verifying it authenticates
 * the CLI; we then mint a fresh session **server-side** and set the cookie. The
 * proof is single-purpose and ~60s-lived, and the real session token never
 * appears in a URL.
 */
export async function GET(req: NextRequest): Promise<NextResponse> {
  const signIn = req.nextUrl.clone();
  signIn.pathname = "/sign-in";
  signIn.search = "";

  const proof = req.nextUrl.searchParams.get("t");
  if (proof === null || !(await verifyHandoffProof(proof, sessionSecret()))) {
    return NextResponse.redirect(signIn);
  }

  const osaToken = mintLocalAdminToken(jwtSecret());
  const value = await createSessionValue(
    { sub: "admin@osa.local", osaToken },
    sessionSecret(),
  );

  const home = req.nextUrl.clone();
  home.pathname = "/";
  home.search = "";
  const res = NextResponse.redirect(home);
  res.cookies.set(SESSION_COOKIE, value, sessionCookieOptions(isSecureRequest(req)));
  return res;
}
