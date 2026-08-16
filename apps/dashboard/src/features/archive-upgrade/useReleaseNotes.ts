"use client";

import { useQuery } from "@tanstack/react-query";

import { upgradeKeys } from "./keys";

export interface ReleaseNote {
  version: string;
  name: string;
  body: string;
  htmlUrl: string;
}

/**
 * Release notes for every version in `(from, to]`, oldest first, via the
 * dashboard's cached GitHub proxy. Failure is non-fatal — the dialog
 * degrades to the registry's notes_url link.
 */
export function useReleaseNotes(from: string, to: string, enabled: boolean) {
  return useQuery({
    queryKey: upgradeKeys.releaseNotes(from, to),
    enabled,
    staleTime: 10 * 60_000,
    retry: false,
    queryFn: async (): Promise<ReleaseNote[]> => {
      const res = await fetch(
        `/api/releases?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
      );
      if (!res.ok) throw new Error(`release notes unavailable (${res.status})`);
      const body = (await res.json()) as { releases: ReleaseNote[] };
      return body.releases;
    },
  });
}
