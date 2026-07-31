import { NextResponse, type NextRequest } from "next/server";

import { refreshTokens } from "@/server/amacrin";
import { amacrinApiUrl, sessionSecret } from "@/server/env";
import { isSecureRequest } from "@/server/request";
import {
  SESSION_COOKIE,
  createPlatformSessionValue,
  readPlatformSession,
  sessionCookieOptions,
  type PlatformSessionData,
} from "@/server/platform-session";

export const runtime = "nodejs";

/**
 * Platform BFF data proxy (issue #185, session-bearing BFF).
 *
 * The browser calls same-origin `/api/amacrin/api/v1/*`; this handler unseals
 * the access token from the session cookie, attaches it as a bearer, and
 * forwards to the in-cluster control plane (`AMACRIN_API_URL`). On a 401 it
 * rotates the token pair once (JSON-body refresh), re-seals the rotated pair
 * into a fresh session cookie, and retries — so an expired access token is
 * invisible to the browser. The user's own token scopes privileges; the proxy
 * only guards the `/api/v1/` prefix (no `..` traversal to another host/path).
 */

/** The parts of the inbound request needed to forward it (body read once). */
interface Forwardable {
  method: string;
  search: string;
  accept: string;
  contentType: string | null;
  body: string | undefined;
}

async function capture(req: NextRequest): Promise<Forwardable> {
  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  return {
    method: req.method,
    search: req.nextUrl.search,
    accept: req.headers.get("accept") ?? "application/json",
    contentType: req.headers.get("content-type"),
    body: hasBody ? await req.text() : undefined,
  };
}

async function forward(
  fwd: Forwardable,
  path: string[],
  accessToken: string,
): Promise<Response | null> {
  const joined = path.join("/");
  const target = new URL(`/${joined}`, amacrinApiUrl());
  // Only forward genuine control-plane API calls; block traversal that would
  // collapse to a different path (e.g. `api/v1/../../foo`).
  if (target.pathname !== `/${joined}` || !target.pathname.startsWith("/api/v1/")) {
    return null;
  }
  target.search = fwd.search;

  const headers = new Headers();
  headers.set("authorization", `Bearer ${accessToken}`);
  headers.set("accept", fwd.accept);
  if (fwd.contentType !== null) headers.set("content-type", fwd.contentType);

  return fetch(target, {
    method: fwd.method,
    headers,
    body: fwd.body,
    redirect: "manual",
  });
}

function relay(upstream: Response, session?: { value: string; secure: boolean }): NextResponse {
  const headers = new Headers();
  const contentType = upstream.headers.get("content-type");
  if (contentType !== null) headers.set("content-type", contentType);
  const res = new NextResponse(upstream.body, { status: upstream.status, headers });
  if (session !== undefined) {
    res.cookies.set(SESSION_COOKIE, session.value, sessionCookieOptions(session.secure));
  }
  return res;
}

async function proxy(req: NextRequest, path: string[]): Promise<NextResponse> {
  const session = await readPlatformSession(
    req.cookies.get(SESSION_COOKIE)?.value,
    sessionSecret(),
  );
  if (session === null) {
    return NextResponse.json({ error: "unauthenticated" }, { status: 401 });
  }

  const fwd = await capture(req);

  const first = await forward(fwd, path, session.accessToken);
  if (first === null) return NextResponse.json({ error: "not_allowed" }, { status: 403 });
  if (first.status !== 401) return relay(first);

  // Access token expired — rotate once (the cloud rotates the refresh token),
  // re-seal, and retry. A dead refresh surfaces the original 401.
  let rotated: PlatformSessionData;
  try {
    const tokens = await refreshTokens(session.refreshToken);
    rotated = { accessToken: tokens.accessToken, refreshToken: tokens.refreshToken };
  } catch {
    return relay(first);
  }

  const retry = await forward(fwd, path, rotated.accessToken);
  if (retry === null) return NextResponse.json({ error: "not_allowed" }, { status: 403 });
  const value = await createPlatformSessionValue(rotated, sessionSecret());
  return relay(retry, { value, secure: isSecureRequest(req) });
}

type Ctx = { params: Promise<{ path: string[] }> };

export async function GET(req: NextRequest, ctx: Ctx): Promise<NextResponse> {
  return proxy(req, (await ctx.params).path);
}

export async function POST(req: NextRequest, ctx: Ctx): Promise<NextResponse> {
  return proxy(req, (await ctx.params).path);
}

export async function DELETE(req: NextRequest, ctx: Ctx): Promise<NextResponse> {
  return proxy(req, (await ctx.params).path);
}
