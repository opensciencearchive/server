import { HttpResponse, http } from "msw";
import { describe, expect, it, vi } from "vitest";

import { server } from "@/mocks/server";

import { HttpClient } from "./client";
import { ApiError } from "./errors";

const BASE = "https://api.test";

function makeClient(opts?: { onUnauthorized?: () => void }) {
  return new HttpClient({ baseUrl: BASE, onUnauthorized: opts?.onUnauthorized });
}

describe("HttpClient", () => {
  it("sends the request and returns parsed JSON", async () => {
    server.use(
      http.get(`${BASE}/api/v1/archives`, () => HttpResponse.json([{ ok: true }])),
    );
    await expect(makeClient().get("/api/v1/archives")).resolves.toEqual([
      { ok: true },
    ]);
  });

  it("fires onUnauthorized and throws on a 401 (the BFF owns refresh)", async () => {
    server.use(
      http.get(`${BASE}/api/v1/archives`, () =>
        HttpResponse.json(
          { error: "unauthorized", message: "session gone" },
          { status: 401 },
        ),
      ),
    );
    const onUnauthorized = vi.fn();
    await expect(
      makeClient({ onUnauthorized }).get("/api/v1/archives"),
    ).rejects.toMatchObject({ status: 401 });
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
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

    const err = await makeClient()
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
      http.get(`${BASE}/api/v1/ping`, () => new HttpResponse(null, { status: 204 })),
    );
    await expect(makeClient().get("/api/v1/ping")).resolves.toBeUndefined();
  });
});
