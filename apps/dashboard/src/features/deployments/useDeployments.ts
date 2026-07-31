"use client";

import { useQuery } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import { isDeploymentSettled } from "@/domain/deployment";

import { archiveKeys } from "../archives/keys";

const POLL_MS = 3000;

/** The archive's deployment history, polling while the newest is unsettled. */
export function useDeployments(archiveId: string) {
  const { amacrin } = usePlatformServices();
  return useQuery({
    queryKey: archiveKeys.deployments(archiveId),
    queryFn: () => amacrin.listDeployments(archiveId),
    refetchInterval: (q) => {
      const newest = q.state.data?.[0];
      return newest && !isDeploymentSettled(newest.status) ? POLL_MS : false;
    },
  });
}
