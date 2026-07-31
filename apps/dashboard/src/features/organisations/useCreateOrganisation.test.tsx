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
    expect(fetchSpy).toHaveBeenCalledWith("/api/auth/refresh", { method: "POST" });
    expect(calls[0]).toBe("fetch:/api/auth/refresh");
    expect(calls.indexOf("fetch:/api/auth/refresh")).toBeLessThan(
      calls.indexOf("invalidate"),
    );
  });
});
