import { beforeEach, describe, expect, it, vi } from "vitest";

import { TokenStore } from "./token-store";

describe("TokenStore", () => {
  beforeEach(() => {
    vi.useRealTimers();
  });

  it("holds a token in memory and returns it", () => {
    const store = new TokenStore();
    expect(store.get()).toBeNull();
    store.set("tok_abc", 900);
    expect(store.get()).toBe("tok_abc");
  });

  it("never writes to web storage", () => {
    const setItem = vi.spyOn(Storage.prototype, "setItem");
    const store = new TokenStore();
    store.set("tok_secret", 900);
    expect(setItem).not.toHaveBeenCalled();
    setItem.mockRestore();
  });

  it("reports an empty store as expiring", () => {
    const store = new TokenStore();
    expect(store.expiresWithin(60_000)).toBe(true);
  });

  it("tracks expiry from expires_in", () => {
    vi.useFakeTimers();
    const store = new TokenStore();
    store.set("tok_abc", 900); // 15 min
    expect(store.expiresWithin(60_000)).toBe(false);
    vi.advanceTimersByTime(850_000); // 14m10s elapsed → 50s left
    expect(store.expiresWithin(60_000)).toBe(true);
  });

  it("clear() drops the token", () => {
    const store = new TokenStore();
    store.set("tok_abc", 900);
    store.clear();
    expect(store.get()).toBeNull();
  });
});
