"use client";

import { useQuery } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import { isBuildKindTerminal, isBuildTerminal } from "@/domain/build";

import { buildKeys } from "./keys";

const POLL_MS = 3000;

/** One build (parent + components), polling while non-terminal. */
export function useBuild(buildId: string) {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: buildKeys.detail(buildId),
    queryFn: () => amacrin.getBuild(buildId),
    refetchInterval: (q) =>
      q.state.data && !isBuildTerminal(q.state.data.status) ? POLL_MS : false,
  });
}

/** The archive's build history, polling while any build is still in flight. */
export function useBuildList(archiveId: string) {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: buildKeys.list(archiveId),
    queryFn: () => amacrin.listBuilds(archiveId),
    refetchInterval: (q) =>
      q.state.data?.some((b) => !isBuildKindTerminal(b.statusKind))
        ? POLL_MS
        : false,
  });
}
