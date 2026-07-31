// @vitest-environment node
import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SESSION_COOKIE, readPlatformSession } from "@/server/platform-session";

import { GET } from "./route";

const SECRET = "session-secret-value-at-least-32-chars!";

vi.mock("@/server/amacrin", () => ({
  exchangeHandoffCode: vi.fn(async (code: string) => {
    if (code === "bad") throw new Error("exchange failed");
    return { accessToken: "acc", refreshToken: "ref", expiresInSeconds: 900 };
  }),
}));

beforeEach(() => {
  vi.stubEnv("SESSION_SECRET", SECRET);
});

afterEach(() => {
  vi.unstubAllEnvs();
});

function callbackRequest(query: string, nonceCookie?: string): NextRequest {
  const headers: Record<string, string> = {};
  if (nonceCookie !== undefined) headers["cookie"] = `osa_signin_nonce=${nonceCookie}`;
  return new NextRequest(`https://console.amacrin.test/api/auth/callback${query}`, {
    headers,
  });
}

describe("platform callback route", () => {
  it("exchanges the code and seals a session when the nonce matches", async () => {
    const res = await GET(callbackRequest("?n=abc&code=hoc_1", "abc"));

    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toBe("/");

    const session = res.cookies.get(SESSION_COOKIE);
    expect(session?.value).toBeTruthy();
    expect(await readPlatformSession(session!.value, SECRET)).toEqual({
      accessToken: "acc",
      refreshToken: "ref",
    });
  });

  it("rejects a callback whose nonce does not match the cookie (login CSRF)", async () => {
    const res = await GET(callbackRequest("?n=attacker&code=hoc_1", "victim"));

    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toBe("/sign-in?error=invalid");
    expect(res.cookies.get(SESSION_COOKIE)?.value ?? "").toBe("");
  });

  it("rejects a callback with no nonce cookie", async () => {
    const res = await GET(callbackRequest("?n=abc&code=hoc_1"));
    expect(res.headers.get("location")).toBe("/sign-in?error=invalid");
  });

  it("surfaces a waitlisted result", async () => {
    const res = await GET(callbackRequest("?n=abc&waitlisted=1", "abc"));
    expect(res.headers.get("location")).toBe("/sign-in?error=waitlisted");
  });

  it("redirects to an error when the exchange fails", async () => {
    const res = await GET(callbackRequest("?n=abc&code=bad", "abc"));
    expect(res.headers.get("location")).toBe("/sign-in?error=provider_error");
    expect(res.cookies.get(SESSION_COOKIE)?.value ?? "").toBe("");
  });
});
