"use client";

import { useQuery } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";

import { upgradeKeys } from "./keys";

/** GET /osa-versions — the registry, newest first. Platform mode only. */
export function useOsaVersions() {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: upgradeKeys.versions,
    queryFn: () => amacrin.listOsaVersions(),
    staleTime: 60_000,
  });
}
