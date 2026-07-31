import { waitFor } from "@testing-library/react";
import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { archiveKeys } from "./keys";
import { renderHookWrapper } from "@/test/render-hook";

import { useCreateArchive } from "./useCreateArchive";

describe("useCreateArchive", () => {
  it("seeds the detail + status caches on success", async () => {
    const { wrapper, queryClient } = renderHookWrapper();
    const { result } = renderHook(() => useCreateArchive(), { wrapper });

    result.current.mutate({
      orgId: "org_7f3k2mq9x1",
      input: {
        name: "Alpine climate network",
        slug: "fresh-atlas",
        orcid: { clientId: "APP-1", clientSecret: "sekret" },
        adminOrcidIds: [],
      },
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const { archive, deployment } = result.current.data!;
    expect(queryClient.getQueryData(archiveKeys.detail(archive.id))).toEqual(
      archive,
    );
    expect(queryClient.getQueryData(archiveKeys.status(archive.id))).toEqual(
      deployment,
    );
  });

  it("rejects with SlugTakenError for a taken slug", async () => {
    const { wrapper } = renderHookWrapper();
    const { result } = renderHook(() => useCreateArchive(), { wrapper });

    result.current.mutate({
      orgId: "org_7f3k2mq9x1",
      input: {
        name: "Duplicate",
        slug: "alpine-climate",
        orcid: { clientId: "APP-1", clientSecret: "sekret" },
        adminOrcidIds: [],
      },
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toMatchObject({
      name: "SlugTakenError",
      slug: "alpine-climate",
    });
  });
});
