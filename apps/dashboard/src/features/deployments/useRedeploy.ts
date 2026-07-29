"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import type { ArchiveAuthInput } from "@/api/amacrin/service";

import { archiveKeys } from "../archives/keys";

/**
 * POST /archives/{id}/deploy. Without `auth` it reuses the sealed
 * credentials and stored admins; with `auth` it rotates both.
 */
export function useRedeploy(archiveId: string) {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (input?: { auth?: ArchiveAuthInput }) =>
      amacrin.deploy(archiveId, input?.auth),
    onSuccess: async (deployment) => {
      queryClient.setQueryData(archiveKeys.status(archiveId), deployment);
      await queryClient.invalidateQueries({
        queryKey: archiveKeys.detail(archiveId),
      });
      await queryClient.invalidateQueries({
        queryKey: archiveKeys.status(archiveId),
      });
    },
  });
}
