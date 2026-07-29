import { HttpResponse, http } from "msw";
import { describe, expect, it, vi } from "vitest";

import { SessionRefresher } from "@/api/auth/refresher";
import { TokenStore } from "@/api/auth/token-store";
import { server } from "@/mocks/server";

import { HttpClient } from "./client";
import { ApiError } from "./errors";

const BASE = "https://api.test";

function makeClient(opts?: {
  token?: string;
  refreshFn?: () => Promise<{ accessToken: string; expiresInSeconds: number }>;
  onSessionLost?: () => void;
}) {
  const store = new TokenStore();
  if (opts?.token) store.set(opts.token, 900);
  const refreshFn =
    opts?.refreshFn ??
    vi.fn().mockResolvedValue({ accessToken: "tok_refreshed", expiresInSeconds: 900 });
  const refresher = new SessionRefresher({
    store,
    refreshFn,
    onSessionLost: opts?.onSessionLost,
  });
  return { client: new HttpClient({ baseUrl: BASE, refresher }), refreshFn, store };
}

describe("HttpClient", () => {
  it("sends the bearer token and returns parsed JSON", async () => {
    let seenAuth: string | null = null;
    server.use(
      http.get(`${BASE}/api/v1/archives`, ({ request }) => {
        seenAuth = request.headers.get("authorization");
        return HttpResponse.json([{ ok: true }]);
      }),
    );
    const { client } = makeClient({ token: "tok_abc" });

    await expect(client.get("/api/v1/archives")).resolves.toEqual([
      { ok: true },
    ]);
    expect(seenAuth).toBe("Bearer tok_abc");
  });

  it("retries exactly once with a fresh token after a 401", async () => {
    let calls = 0;
    server.use(
      http.get(`${BASE}/api/v1/archives`, ({ request }) => {
        calls += 1;
        if (request.headers.get("authorization") === "Bearer tok_refreshed") {
          return HttpResponse.json([]);
        }
        return HttpResponse.json(
          { error: "unauthorized", message: "expired", reason: "expired" },
          { status: 401 },
        );
      }),
    );
    const { client, refreshFn } = makeClient({ token: "tok_stale_but_looks_fresh" });

    await expect(client.get("/api/v1/archives")).resolves.toEqual([]);
    expect(calls).toBe(2);
    expect(refreshFn).toHaveBeenCalledTimes(1);
  });

  it("gives up after the retried request also 401s", async () => {
    server.use(
      http.get(`${BASE}/api/v1/archives`, () =>
        HttpResponse.json(
          { error: "unauthorized", message: "expired", reason: "expired" },
          { status: 401 },
        ),
      ),
    );
    const { client, refreshFn } = makeClient({ token: "tok_zombie" });

    await expect(client.get("/api/v1/archives")).rejects.toMatchObject({
      status: 401,
    });
    expect(refreshFn).toHaveBeenCalledTimes(1); // once, not a loop
  });

  it("treats a dead refresh as session lost without retrying", async () => {
    server.use(
      http.get(`${BASE}/api/v1/archives`, () =>
        HttpResponse.json(
          { error: "unauthorized", message: "expired", reason: "expired" },
          { status: 401 },
        ),
      ),
    );
    const onSessionLost = vi.fn();
    const { client } = makeClient({
      token: "tok_zombie",
      refreshFn: vi.fn().mockRejectedValue(
        new ApiError({ status: 401, code: "unauthorized", message: "revoked" }),
      ),
      onSessionLost,
    });

    await expect(client.get("/api/v1/archives")).rejects.toMatchObject({
      status: 401,
    });
    expect(onSessionLost).toHaveBeenCalled();
  });

  it("maps error bodies to ApiError with the request id", async () => {
    server.use(
      http.post(`${BASE}/api/v1/organisations`, () =>
        HttpResponse.json(
          { error: "duplicate", message: "name taken" },
          { status: 409, headers: { "x-request-id": "req_123" } },
        ),
      ),
    );
    const { client } = makeClient({ token: "tok_abc" });

    const err = await client
      .post("/api/v1/organisations", { name: "X" })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    const apiErr = err as ApiError;
    expect(apiErr.status).toBe(409);
    expect(apiErr.code).toBe("duplicate");
    expect(apiErr.message).toBe("name taken");
    expect(apiErr.requestId).toBe("req_123");
  });

  it("returns undefined for 204 responses", async () => {
    server.use(
      http.post(`${BASE}/api/v1/auth/logout`, () =>
        new HttpResponse(null, { status: 204 }),
      ),
    );
    const { client } = makeClient({ token: "tok_abc" });

    await expect(client.post("/api/v1/auth/logout")).resolves.toBeUndefined();
  });
});
