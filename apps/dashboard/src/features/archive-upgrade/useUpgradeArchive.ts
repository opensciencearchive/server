"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";

import { usePlatformServices } from "@/api/services";

import { archiveKeys } from "../archives/keys";

/**
 * POST /archives/{id}/upgrade — forward-only. On the 202, the deployment is
 * pre-seeded into the status cache (mirroring useCreateArchive) and the user
 * is handed to /deploying/{id}, which owns progress from there.
 */
export function useUpgradeArchive(archiveId: string) {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();
  const router = useRouter();

  return useMutation({
    mutationFn: (toVersion: string) =>
      amacrin.upgradeArchive(archiveId, toVersion),
    onSuccess: async (deployment) => {
      queryClient.setQueryData(archiveKeys.status(archiveId), deployment);
      await queryClient.invalidateQueries({
        queryKey: archiveKeys.detail(archiveId),
      });
      router.push(`/deploying/${archiveId}`);
    },
  });
}
