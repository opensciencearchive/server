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
/**
 * Redirect with a RELATIVE Location. `NextResponse.redirect` in a node route
 * handler builds an absolute URL from `req.nextUrl`, whose host is the server's
 * internal bind (0.0.0.0:3000 in the container), not the address the browser
 * actually used (e.g. localhost:8081 behind a port-map). A relative Location
 * lets the browser resolve it against the request URL, so it stays on the right
 * host.
 */
function redirectTo(path: string): NextResponse {
  return new NextResponse(null, { status: 303, headers: { location: path } });
}

export async function GET(req: NextRequest): Promise<NextResponse> {
  const proof = req.nextUrl.searchParams.get("t");
  if (proof === null || !(await verifyHandoffProof(proof, sessionSecret()))) {
    return redirectTo("/sign-in");
  }

  const osaToken = mintLocalAdminToken(jwtSecret());
  const value = await createSessionValue(
    { sub: "admin@osa.local", osaToken },
    sessionSecret(),
  );

  const res = redirectTo("/");
  res.cookies.set(SESSION_COOKIE, value, sessionCookieOptions(isSecureRequest(req)));
  return res;
}
