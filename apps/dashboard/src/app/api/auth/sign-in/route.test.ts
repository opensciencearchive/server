// @vitest-environment node
import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SIGNIN_NONCE_COOKIE } from "@/server/platform-session";

import { GET } from "./route";

const PUBLIC_API = "https://api.amacrin.test";

beforeEach(() => {
  vi.stubEnv("AMACRIN_API_URL", "http://amacrin-api.svc");
  vi.stubEnv("AMACRIN_PUBLIC_URL", PUBLIC_API);
});

afterEach(() => {
  vi.unstubAllEnvs();
});

function signInRequest(): NextRequest {
  return new NextRequest("https://console.amacrin.test/api/auth/sign-in", {
    headers: {
      "x-forwarded-proto": "https",
      "x-forwarded-host": "console.amacrin.test",
    },
  });
}

describe("platform sign-in route", () => {
  it("redirects to the control-plane login with a nonce round-tripped via redirect_uri", async () => {
    const res = await GET(signInRequest());

    expect(res.status).toBe(302);
    const location = new URL(res.headers.get("location")!);
    expect(location.origin).toBe(PUBLIC_API);
    expect(location.pathname).toBe("/api/v1/auth/login");
    expect(location.searchParams.get("provider")).toBe("google");

    const redirectUri = new URL(location.searchParams.get("redirect_uri")!);
    expect(redirectUri.origin).toBe("https://console.amacrin.test");
    expect(redirectUri.pathname).toBe("/api/auth/callback");
    const nonce = redirectUri.searchParams.get("n");
    expect(nonce).toBeTruthy();

    // The same nonce is set as an httpOnly cookie for the callback to verify.
    const cookie = res.cookies.get(SIGNIN_NONCE_COOKIE);
    expect(cookie?.value).toBe(nonce);
    expect(cookie?.httpOnly).toBe(true);
  });
});
