import { NextResponse, type NextRequest } from "next/server";

import { isSameOrigin } from "@/server/request";
import { SESSION_COOKIE } from "@/server/session";

export const runtime = "nodejs";

/** Clear the dashboard session cookie. Idempotent. */
export async function POST(req: NextRequest): Promise<NextResponse> {
  if (!isSameOrigin(req)) {
    return NextResponse.json({ error: "invalid_origin" }, { status: 403 });
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
