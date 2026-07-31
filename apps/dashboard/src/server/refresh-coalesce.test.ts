// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";

import { refreshTokens } from "./amacrin";
import { coalesceRefresh } from "./refresh-coalesce";

vi.mock("./amacrin", () => ({
  refreshTokens: vi.fn(),
}));

const mockRefresh = vi.mocked(refreshTokens);

afterEach(() => {
  vi.clearAllMocks();
  vi.useRealTimers();
});

function tokens(access: string) {
  return { accessToken: access, refreshToken: `${access}-r`, expiresInSeconds: 900 };
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

// Distinct tokens per test: the module holds per-token in-flight + result-cache
// state, so reusing a token would leak across tests.
describe("coalesceRefresh", () => {
  it("collapses concurrent refreshes of the same token into one upstream call", async () => {
    const d = deferred<ReturnType<typeof tokens>>();
    mockRefresh.mockReturnValueOnce(d.promise);

    const a = coalesceRefresh("t-concurrent");
    const b = coalesceRefresh("t-concurrent");

    expect(mockRefresh).toHaveBeenCalledTimes(1);
    d.resolve(tokens("acc2"));

    const [ra, rb] = await Promise.all([a, b]);
    expect(ra).toBe(rb);
    expect(ra.accessToken).toBe("acc2");
  });

  it("serves the just-rotated pair from cache to a delayed caller (no upstream reuse)", async () => {
    mockRefresh.mockResolvedValue(tokens("acc2"));

    // Winner rotates and settles; a caller that still holds the old token (its
    // cookie not yet updated) must get the rotated pair from cache, not re-submit.
    const first = await coalesceRefresh("t-cache");
    const delayed = await coalesceRefresh("t-cache");

    expect(mockRefresh).toHaveBeenCalledTimes(1);
    expect(delayed).toEqual(first);
    expect(delayed.accessToken).toBe("acc2");
  });

  it("refreshes again once the cached result expires past the grace window", async () => {
    vi.useFakeTimers();
    mockRefresh.mockResolvedValue(tokens("acc"));

    await coalesceRefresh("t-expiry");
    await vi.advanceTimersByTimeAsync(30_001);
    await coalesceRefresh("t-expiry");

    expect(mockRefresh).toHaveBeenCalledTimes(2);
  });

  it("does not coalesce different tokens", async () => {
    mockRefresh.mockResolvedValue(tokens("acc"));

    await Promise.all([coalesceRefresh("t-a"), coalesceRefresh("t-b")]);
    expect(mockRefresh).toHaveBeenCalledTimes(2);
  });
});
