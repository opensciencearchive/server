import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { makeTestServices } from "@/test/render";
import { renderHookWrapper } from "@/test/render-hook";

import { useCreateOrganisation } from "./useCreateOrganisation";

describe("useCreateOrganisation", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("refreshes the session via the BFF BEFORE invalidating queries (org claims ride the token)", async () => {
    const services = makeTestServices();
    const calls: string[] = [];

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation((input: RequestInfo | URL) => {
        calls.push(`fetch:${String(input)}`);
        return Promise.resolve(new Response(JSON.stringify({ ok: true })));
      });

    const { wrapper, queryClient } = renderHookWrapper({ services });
    vi.spyOn(queryClient, "invalidateQueries").mockImplementation(() => {
      calls.push("invalidate");
      return Promise.resolve();
    });

    const { result } = renderHook(() => useCreateOrganisation(), { wrapper });
    result.current.mutate({ name: "New Lab" });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.sessionRefreshed).toBe(true);
    expect(fetchSpy).toHaveBeenCalledWith("/api/auth/refresh", { method: "POST" });
    expect(calls[0]).toBe("fetch:/api/auth/refresh");
    expect(calls.indexOf("fetch:/api/auth/refresh")).toBeLessThan(
      calls.indexOf("invalidate"),
    );
  });

  it("still succeeds (no double-create) but flags sessionRefreshed=false when refresh fails", async () => {
    const services = makeTestServices();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(null, { status: 401 }),
    );

    const { wrapper, queryClient } = renderHookWrapper({ services });
    const invalidateSpy = vi
      .spyOn(queryClient, "invalidateQueries")
      .mockResolvedValue(undefined);

    const { result } = renderHook(() => useCreateOrganisation(), { wrapper });
    result.current.mutate({ name: "New Lab" });

    // A failed refresh must NOT become a mutation error (that would re-enable the
    // form and risk a duplicate create). It's a success with sessionRefreshed=false.
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.isError).toBe(false);
    expect(result.current.data?.sessionRefreshed).toBe(false);
    expect(result.current.data?.created).toBeDefined();
    expect(invalidateSpy).not.toHaveBeenCalled();
  });
});
