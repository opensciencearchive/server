import { NextResponse, type NextRequest } from "next/server";

import { isPlatformFromEnv } from "@/api/config";
import { compareOsaVersions } from "@/domain/osa-version";
import { sessionSecret } from "@/server/env";
import { readPlatformSession } from "@/server/platform-session";
import { RELEASE_TAG, publishedReleases } from "@/server/releases";
import { SESSION_COOKIE, readSession } from "@/server/session";

export const runtime = "nodejs";

/**
 * BFF: release notes for the upgrade dialog (#222).
 *
 * `GET /api/releases?from=v0.0.10&to=v0.0.12` → the notes of every release in
 * `(from, to]`, oldest first, so multi-version jumps read chronologically.
 * GitHub failure is a 502 the dialog degrades on (registry `notes_url`
 * link-out).
 */

async function authorized(req: NextRequest): Promise<boolean> {
  const cookie = req.cookies.get(SESSION_COOKIE)?.value;
  if (isPlatformFromEnv()) {
    return (await readPlatformSession(cookie, sessionSecret())) !== null;
  }
  return (await readSession(cookie, sessionSecret())) !== null;
}

export async function GET(req: NextRequest): Promise<NextResponse> {
  if (!(await authorized(req))) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  const from = req.nextUrl.searchParams.get("from") ?? "";
  const to = req.nextUrl.searchParams.get("to") ?? "";
  if (!RELEASE_TAG.test(to)) {
    return NextResponse.json({ error: "invalid_range" }, { status: 400 });
  }

  try {
    const releases = (await publishedReleases())
      .filter(
        (r) =>
          compareOsaVersions(r.version, to) <= 0 &&
          (from === "" || compareOsaVersions(r.version, from) > 0),
      )
      .sort((a, b) => compareOsaVersions(a.version, b.version));
    return NextResponse.json({ releases });
  } catch {
    return NextResponse.json({ error: "github_unavailable" }, { status: 502 });
  }
}
