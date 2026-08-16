// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { latestOsaVersion, resetLatestOsaVersionCache } from "./ghcr";

describe("latestOsaVersion", () => {
  beforeEach(() => resetLatestOsaVersionCache());
  afterEach(() => vi.restoreAllMocks());

  it("passes an abort signal so a stalled registry cannot block /api/node", async () => {
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(Response.json({ token: "t" }))
      .mockResolvedValueOnce(Response.json({ tags: ["v0.0.11", "v0.0.9"] }));

    await expect(latestOsaVersion()).resolves.toBe("v0.0.11");
    for (const call of spy.mock.calls) {
      expect(call[1]?.signal).toBeInstanceOf(AbortSignal);
    }
  });

  it("degrades to null when the lookup aborts", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(
      new DOMException("The operation timed out.", "TimeoutError"),
    );
    await expect(latestOsaVersion()).resolves.toBeNull();
  });
});
