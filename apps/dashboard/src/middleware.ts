import { NextResponse, type NextRequest } from "next/server";

import { SELF_HOST_ARCHIVE_ID } from "@/api/config";
import { sessionSecret } from "@/server/env";
import { SESSION_COOKIE, readSession } from "@/server/session";

/**
 * Self-host request gate (issue #173).
 *
 * For a self-hosted build (`IS_PLATFORM=false`):
 *  1. Require a valid dashboard session cookie; unauthenticated requests go to
 *     `/sign-in` (which is excluded from the matcher, so it stays reachable).
 *  2. Collapse the (nonexistent) fleet routes — `/`, `/organisations/*`,
 *     `/archives/new` — onto the single archive root.
 *
 * Verification uses `jose` (edge-safe). A platform build keeps client-side
 * auth (`AuthGuard`) and does nothing here.
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

export async function middleware(req: NextRequest): Promise<NextResponse> {
  if (IS_PLATFORM) return NextResponse.next();

  const session = await readSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    const url = req.nextUrl.clone();
    url.pathname = "/sign-in";
    return NextResponse.redirect(url);
  }

  if (isFleetPath(req.nextUrl.pathname)) {
    const url = req.nextUrl.clone();
    url.pathname = SELF_HOST_HOME;
    return NextResponse.redirect(url);
  }

  return NextResponse.next();
}

export const config = {
  // Guard everything except API routes, Next internals, and the public
  // sign-in page.
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico|sign-in).*)"],
};
