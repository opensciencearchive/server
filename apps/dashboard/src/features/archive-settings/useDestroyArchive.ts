"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";

import { usePlatformServices } from "@/api/services";

import { archiveKeys } from "../archives/keys";
import { orgKeys } from "../organisations/keys";

/**
 * POST /archives/{id}/destroy — the only removal path (Owner only). On success
 * invalidate the archive lists and route back to the organisation, where the
 * archive now shows `destroying`.
 */
export function useDestroyArchive(archiveId: string, organisationId: string) {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();
  const router = useRouter();

  return useMutation({
    mutationFn: () => amacrin.destroyArchive(archiveId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: archiveKeys.list }),
        queryClient.invalidateQueries({
          queryKey: orgKeys.archives(organisationId),
        }),
      ]);
      router.push(`/organisations/${organisationId}`);
    },
  });
}
