"use client";

import { useQuery } from "@tanstack/react-query";

import {
  requirePlatform,
  usePlatformServices,
  useServices,
} from "@/api/services";

import { archiveKeys } from "./keys";
import { syntheticLocalArchive } from "./local-archive";

/**
 * Every archive the caller can access, across all their organisations.
 * Platform-only — self-host has exactly one archive (see `useArchive`).
 */
export function useArchives() {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: archiveKeys.list,
    queryFn: () => amacrin.listArchives(),
  });
}

/**
 * A single archive. Shared across both modes: platform reads the cloud archive
 * record; self-host returns the synthetic local archive (this server).
 */
export function useArchive(archiveId: string) {
  const services = useServices();
  return useQuery({
    queryKey: archiveKeys.detail(archiveId),
    queryFn: () =>
      services.isPlatform
        ? requirePlatform(services).amacrin.getArchive(archiveId)
        : Promise.resolve(syntheticLocalArchive()),
  });
}
