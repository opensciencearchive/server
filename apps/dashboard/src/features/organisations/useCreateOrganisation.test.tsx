import { renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { makeTestServices } from "@/test/render";
import { renderHookWrapper } from "@/test/render-hook";

import { useCreateOrganisation } from "./useCreateOrganisation";

describe("useCreateOrganisation", () => {
  it("refreshes the access token BEFORE invalidating session queries (org claims ride the token)", async () => {
    const services = makeTestServices();
    const calls: string[] = [];
    const refreshSpy = vi
      .spyOn(services.refresher, "forceRefresh")
      .mockImplementation(() => {
        calls.push("refresh");
        return Promise.resolve("tok_new");
      });

    const { wrapper, queryClient } = renderHookWrapper({ services });
    const invalidateSpy = vi
      .spyOn(queryClient, "invalidateQueries")
      .mockImplementation(() => {
        calls.push("invalidate");
        return Promise.resolve();
      });

    const { result } = renderHook(() => useCreateOrganisation(), { wrapper });
    result.current.mutate({ name: "New Lab" });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(refreshSpy).toHaveBeenCalled();
    expect(invalidateSpy).toHaveBeenCalled();
    expect(calls[0]).toBe("refresh");
    expect(calls.indexOf("refresh")).toBeLessThan(calls.indexOf("invalidate"));
  });
});
