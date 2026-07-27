"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";

import { usePlatformServices } from "@/api/services";
import { isDeploymentSettled } from "@/domain/deployment";

import { archiveKeys } from "../archives/keys";
import { orgKeys } from "../organisations/keys";

const POLL_MS = 3000;

/**
 * Latest deployment for an archive, polling every 3 s while unsettled.
 * On settling, the archive detail + lists are invalidated so the status
 * flip (deploying → running/error) propagates everywhere.
 */
export function useDeploymentStatus(
  archiveId: string,
  opts?: { enabled?: boolean },
) {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();

  const query = useQuery({
    queryKey: archiveKeys.status(archiveId),
    queryFn: () => amacrin.getDeploymentStatus(archiveId),
    enabled: opts?.enabled ?? true,
    refetchInterval: (q) =>
      q.state.data && !isDeploymentSettled(q.state.data.status)
        ? POLL_MS
        : false,
  });

  const settled = query.data ? isDeploymentSettled(query.data.status) : false;

  useEffect(() => {
    if (!settled) return;
    void queryClient.invalidateQueries({
      queryKey: archiveKeys.detail(archiveId),
    });
    void queryClient.invalidateQueries({ queryKey: archiveKeys.list });
    void queryClient.invalidateQueries({ queryKey: orgKeys.list });
  }, [settled, archiveId, queryClient]);

  return query;
}
