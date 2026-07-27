"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import type { CreateArchiveInput } from "@/api/amacrin/service";
import type { Archive } from "@/domain/archive";
import type { Deployment } from "@/domain/deployment";

import { orgKeys } from "../organisations/keys";
import { archiveKeys } from "./keys";

/**
 * Atomic create-and-deploy (POST /organisations/{id}/archives → 202). On
 * success we seed the detail + status caches with the returned archive and
 * deployment so the overview page renders "deploying" instantly, then
 * invalidate the two lists that now have a new member.
 */
export function useCreateArchive() {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      orgId,
      input,
    }: {
      orgId: string;
      input: CreateArchiveInput;
    }): Promise<{ archive: Archive; deployment: Deployment }> =>
      amacrin.createArchive(orgId, input),
    onSuccess: async ({ archive, deployment }, { orgId }) => {
      queryClient.setQueryData(archiveKeys.detail(archive.id), archive);
      queryClient.setQueryData(archiveKeys.status(archive.id), deployment);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: archiveKeys.list }),
        queryClient.invalidateQueries({ queryKey: orgKeys.archives(orgId) }),
      ]);
    },
  });
}
