// @vitest-environment node
import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  SESSION_COOKIE,
  createPlatformSessionValue,
  readPlatformSession,
} from "@/server/platform-session";

import { GET } from "./route";

const API = "http://amacrin-api.svc";
const SECRET = "session-secret-value-at-least-32-chars!";

vi.mock("@/server/amacrin", () => ({
  refreshTokens: vi.fn(async () => ({
    accessToken: "acc2",
    refreshToken: "ref2",
    expiresInSeconds: 900,
  })),
}));

beforeEach(() => {
  vi.stubEnv("AMACRIN_API_URL", API);
  vi.stubEnv("SESSION_SECRET", SECRET);
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

async function requestWithSession(
  path: string,
  session?: { accessToken: string; refreshToken: string },
): Promise<NextRequest> {
  const headers: Record<string, string> = {};
  if (session !== undefined) {
    const cookie = await createPlatformSessionValue(session, SECRET);
    headers["cookie"] = `${SESSION_COOKIE}=${cookie}`;
  }
  return new NextRequest(`https://console.amacrin.test/api/amacrin/${path}`, { headers });
}

function ctx(path: string[]): { params: Promise<{ path: string[] }> } {
  return { params: Promise.resolve({ path }) };
}

describe("platform data proxy", () => {
  it("401s when there is no session", async () => {
    const req = await requestWithSession("api/v1/organisations");
    const res = await GET(req, ctx(["api", "v1", "organisations"]));
    expect(res.status).toBe(401);
  });

  it("attaches the sealed access token and relays the upstream response", async () => {
    let seenAuth: string | null = null;
    let seenUrl = "";
    vi.spyOn(globalThis, "fetch").mockImplementation((url, init) => {
      seenUrl = String(url);
      seenAuth = new Headers(init?.headers).get("authorization");
      return Promise.resolve(Response.json([{ id: "org_1" }]));
    });

    const req = await requestWithSession("api/v1/organisations", {
      accessToken: "acc1",
      refreshToken: "ref1",
    });
    const res = await GET(req, ctx(["api", "v1", "organisations"]));

    expect(res.status).toBe(200);
    expect(seenUrl).toBe(`${API}/api/v1/organisations`);
    expect(seenAuth).toBe("Bearer acc1");
  });

  it("refreshes, re-seals, and retries once on a 401", async () => {
    const tokens: string[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation((_url, init) => {
      const auth = new Headers(init?.headers).get("authorization") ?? "";
      tokens.push(auth);
      // First call (stale token) 401s; the retry with the rotated token succeeds.
      if (auth === "Bearer acc2") return Promise.resolve(Response.json({ ok: true }));
      return Promise.resolve(new Response(null, { status: 401 }));
    });

    const req = await requestWithSession("api/v1/archives", {
      accessToken: "acc1",
      refreshToken: "ref1",
    });
    const res = await GET(req, ctx(["api", "v1", "archives"]));

    expect(res.status).toBe(200);
    expect(tokens).toEqual(["Bearer acc1", "Bearer acc2"]);

    // The rotated pair is re-sealed into a fresh session cookie.
    const cookie = res.cookies.get(SESSION_COOKIE);
    expect(await readPlatformSession(cookie!.value, SECRET)).toEqual({
      accessToken: "acc2",
      refreshToken: "ref2",
    });
  });

  it("403s a path that escapes the /api/v1 prefix", async () => {
    const req = await requestWithSession("api/v1/../../secrets", {
      accessToken: "acc1",
      refreshToken: "ref1",
    });
    const res = await GET(req, ctx(["api", "v1", "..", "..", "secrets"]));
    expect(res.status).toBe(403);
  });
});
