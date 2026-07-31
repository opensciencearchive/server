// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { exchangeHandoffCode, refreshTokens, revokeSession } from "./amacrin";

const API = "http://amacrin-api.svc";

beforeEach(() => {
  vi.stubEnv("AMACRIN_API_URL", API);
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function mockFetch(handler: (url: string, init: RequestInit) => Response) {
  vi.spyOn(globalThis, "fetch").mockImplementation((input, init) =>
    Promise.resolve(handler(String(input), init ?? {})),
  );
}

describe("amacrin token client", () => {
  it("exchanges a handoff code for a token pair", async () => {
    let seenUrl = "";
    let seenBody: unknown;
    mockFetch((url, init) => {
      seenUrl = url;
      seenBody = JSON.parse(String(init.body));
      return Response.json({
        access_token: "acc",
        refresh_token: "ref",
        expires_in: 900,
        token_type: "Bearer",
      });
    });

    const tokens = await exchangeHandoffCode("hoc_xyz");
    expect(seenUrl).toBe(`${API}/api/v1/auth/token`);
    expect(seenBody).toEqual({ code: "hoc_xyz" });
    expect(tokens).toEqual({
      accessToken: "acc",
      refreshToken: "ref",
      expiresInSeconds: 900,
    });
  });

  it("refreshes with the JSON-body refresh flow", async () => {
    let seenBody: unknown;
    mockFetch((_url, init) => {
      seenBody = JSON.parse(String(init.body));
      return Response.json({ access_token: "a2", refresh_token: "r2", expires_in: 900 });
    });
    const tokens = await refreshTokens("ref-old");
    expect(seenBody).toEqual({ refresh_token: "ref-old" });
    expect(tokens.refreshToken).toBe("r2");
  });

  it("throws on a non-2xx exchange", async () => {
    mockFetch(() => new Response("nope", { status: 401 }));
    await expect(exchangeHandoffCode("bad")).rejects.toThrow(/handoff exchange failed/);
  });

  it("throws on a malformed token response", async () => {
    mockFetch(() => Response.json({ access_token: "a" })); // missing refresh_token
    await expect(refreshTokens("r")).rejects.toThrow(/malformed token response/);
  });

  it("treats logout as idempotent (a 401 is not an error)", async () => {
    mockFetch(() => new Response(null, { status: 401 }));
    await expect(revokeSession("r")).resolves.toBeUndefined();
  });
});
