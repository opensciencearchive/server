"use client";

import { useQuery } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";

import { orgKeys } from "./keys";

export function useOrganisations() {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: orgKeys.list,
    queryFn: () => amacrin.listOrganisations(),
  });
}

export function useOrganisation(orgId: string) {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: orgKeys.detail(orgId),
    queryFn: () => amacrin.getOrganisation(orgId),
  });
}

export function useOrgArchives(orgId: string) {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: orgKeys.archives(orgId),
    queryFn: () => amacrin.listOrgArchives(orgId),
  });
}
