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
});

function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("coalesceRefresh", () => {
  it("collapses concurrent refreshes of the same token into one upstream call", async () => {
    const d = deferred<{ accessToken: string; refreshToken: string; expiresInSeconds: number }>();
    mockRefresh.mockReturnValueOnce(d.promise);

    const a = coalesceRefresh("ref-1");
    const b = coalesceRefresh("ref-1");

    expect(mockRefresh).toHaveBeenCalledTimes(1);
    d.resolve({ accessToken: "acc2", refreshToken: "ref2", expiresInSeconds: 900 });

    const [ra, rb] = await Promise.all([a, b]);
    expect(ra).toBe(rb); // same shared result
    expect(ra.accessToken).toBe("acc2");
  });

  it("refreshes afresh once the in-flight call settles", async () => {
    mockRefresh.mockResolvedValue({
      accessToken: "acc",
      refreshToken: "ref",
      expiresInSeconds: 900,
    });

    await coalesceRefresh("ref-1");
    await coalesceRefresh("ref-1");

    expect(mockRefresh).toHaveBeenCalledTimes(2); // sequential, not coalesced
  });

  it("does not coalesce different tokens", async () => {
    mockRefresh.mockResolvedValue({
      accessToken: "acc",
      refreshToken: "ref",
      expiresInSeconds: 900,
    });

    await Promise.all([coalesceRefresh("ref-a"), coalesceRefresh("ref-b")]);
    expect(mockRefresh).toHaveBeenCalledTimes(2);
  });
});
