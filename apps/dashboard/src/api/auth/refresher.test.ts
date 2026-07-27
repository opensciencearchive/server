import { describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/http/errors";

import { SessionRefresher } from "./refresher";
import { TokenStore } from "./token-store";

function unauthorized(): ApiError {
  return new ApiError({
    status: 401,
    code: "unauthorized",
    message: "session expired or revoked — sign in again",
    reason: "invalid",
  });
}

describe("SessionRefresher", () => {
  it("returns the stored token without refreshing when fresh", async () => {
    const store = new TokenStore();
    store.set("tok_fresh", 900);
    const refreshFn = vi.fn();
    const refresher = new SessionRefresher({ store, refreshFn });

    await expect(refresher.ensureFreshToken()).resolves.toBe("tok_fresh");
    expect(refreshFn).not.toHaveBeenCalled();
  });

  it("refreshes when the token is close to expiry", async () => {
    const store = new TokenStore();
    store.set("tok_stale", 30); // 30s left — inside the 60s window
    const refreshFn = vi
      .fn()
      .mockResolvedValue({ accessToken: "tok_new", expiresInSeconds: 900 });
    const refresher = new SessionRefresher({ store, refreshFn });

    await expect(refresher.ensureFreshToken()).resolves.toBe("tok_new");
    expect(refreshFn).toHaveBeenCalledTimes(1);
    expect(store.get()).toBe("tok_new");
  });

  it("single-flight: concurrent callers share one refresh", async () => {
    const store = new TokenStore();
    let release!: (v: { accessToken: string; expiresInSeconds: number }) => void;
    const refreshFn = vi.fn().mockImplementation(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    const refresher = new SessionRefresher({ store, refreshFn });

    const results = Promise.all([
      refresher.ensureFreshToken(),
      refresher.ensureFreshToken(),
      refresher.forceRefresh(),
    ]);
    release({ accessToken: "tok_one", expiresInSeconds: 900 });

    await expect(results).resolves.toEqual(["tok_one", "tok_one", "tok_one"]);
    expect(refreshFn).toHaveBeenCalledTimes(1);
  });

  it("a later forceRefresh after settlement starts a new flight", async () => {
    const store = new TokenStore();
    const refreshFn = vi
      .fn()
      .mockResolvedValueOnce({ accessToken: "tok_1", expiresInSeconds: 900 })
      .mockResolvedValueOnce({ accessToken: "tok_2", expiresInSeconds: 900 });
    const refresher = new SessionRefresher({ store, refreshFn });

    await expect(refresher.forceRefresh()).resolves.toBe("tok_1");
    await expect(refresher.forceRefresh()).resolves.toBe("tok_2");
    expect(refreshFn).toHaveBeenCalledTimes(2);
  });

  it("a 401 from the refresh endpoint means session lost: clears the store, notifies, returns null", async () => {
    const store = new TokenStore();
    store.set("tok_old", 900);
    const onSessionLost = vi.fn();
    const refreshFn = vi.fn().mockRejectedValue(unauthorized());
    const refresher = new SessionRefresher({ store, refreshFn, onSessionLost });

    await expect(refresher.forceRefresh()).resolves.toBeNull();
    expect(store.get()).toBeNull();
    expect(onSessionLost).toHaveBeenCalledTimes(1);
  });

  it("network failures propagate without killing the session", async () => {
    const store = new TokenStore();
    store.set("tok_old", 900);
    const onSessionLost = vi.fn();
    const refreshFn = vi.fn().mockRejectedValue(new TypeError("fetch failed"));
    const refresher = new SessionRefresher({ store, refreshFn, onSessionLost });

    await expect(refresher.forceRefresh()).rejects.toThrow("fetch failed");
    expect(store.get()).toBe("tok_old");
    expect(onSessionLost).not.toHaveBeenCalled();
  });
});
