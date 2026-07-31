import { NextResponse, type NextRequest } from "next/server";

import { SELF_HOST_ARCHIVE_ID } from "@/api/config";
import { sessionSecret } from "@/server/env";
import { readPlatformSession } from "@/server/platform-session";
import { SESSION_COOKIE, readSession } from "@/server/session";

/**
 * Dashboard request gate.
 *
 * Both builds now hold a same-origin httpOnly session cookie, so both are
 * guarded here (edge-safe `jose` verification):
 *  - **platform** (`IS_PLATFORM=true`, #185): require a valid sealed token-pair
 *    session; unauthenticated requests go to `/sign-in`. No fleet collapse —
 *    the fleet routes are real.
 *  - **self-host** (`IS_PLATFORM=false`, #173): require a valid session, then
 *    collapse the (nonexistent) fleet routes — `/`, `/organisations/*`,
 *    `/archives/new` — onto the single archive root.
 *
 * `/sign-in` and API routes are excluded from the matcher, so they stay
 * reachable while signed out.
 */
const IS_PLATFORM = process.env.NEXT_PUBLIC_IS_PLATFORM === "true";
const SELF_HOST_HOME = `/archives/${SELF_HOST_ARCHIVE_ID}`;

function isFleetPath(pathname: string): boolean {
  return (
    pathname === "/" ||
    pathname === "/archives/new" ||
    pathname.startsWith("/organisations")
  );
}

function redirectToSignIn(req: NextRequest): NextResponse {
  const url = req.nextUrl.clone();
  url.pathname = "/sign-in";
  return NextResponse.redirect(url);
}

export async function middleware(req: NextRequest): Promise<NextResponse> {
  const secret = sessionSecret();
  const cookie = req.cookies.get(SESSION_COOKIE)?.value;

  if (IS_PLATFORM) {
    const session = await readPlatformSession(cookie, secret);
    return session === null ? redirectToSignIn(req) : NextResponse.next();
  }

  const session = await readSession(cookie, secret);
  if (session === null) {
    return redirectToSignIn(req);
  }

  if (isFleetPath(req.nextUrl.pathname)) {
    const url = req.nextUrl.clone();
    url.pathname = SELF_HOST_HOME;
    return NextResponse.redirect(url);
  }

  return NextResponse.next();
}

export const config = {
  // Guard everything except API routes, Next internals, the public sign-in
  // page, and static files (any path with a file extension — `public/`
  // assets like /osa-logo.svg must load on the signed-out sign-in page;
  // without the `.*\..*` exclusion the guard 307s them to /sign-in and
  // images render broken).
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico|sign-in|.*\\..*).*)"],
};
