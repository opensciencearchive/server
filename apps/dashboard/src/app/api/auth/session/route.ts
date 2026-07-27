import { NextResponse, type NextRequest } from "next/server";

import { sessionSecret } from "@/server/env";
import { SESSION_COOKIE, readSession } from "@/server/session";

export const runtime = "nodejs";

/** Report whether the caller has a valid dashboard session (for AuthGuard). */
export async function GET(req: NextRequest): Promise<NextResponse> {
  const session = await readSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ authenticated: false });
  }
  return NextResponse.json({
    authenticated: true,
    user: { email: session.sub },
  });
}
