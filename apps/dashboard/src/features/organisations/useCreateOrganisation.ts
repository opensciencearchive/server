"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import type { Organisation } from "@/domain/organisation";

import { authKeys } from "../auth/keys";
import { orgKeys } from "./keys";

export function useCreateOrganisation() {
  const { amacrin, refresher } = usePlatformServices();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name }: { name: string }) => amacrin.createOrganisation(name),
    onSuccess: async (created: Organisation) => {
      // Org permissions ride the access token — without this refresh the
      // new organisation 404s until the old token expires. Order matters:
      // refresh FIRST, then refetch queries with the new claims.
      await refresher.forceRefresh();
      await queryClient.invalidateQueries({ queryKey: authKeys.session });
      await queryClient.invalidateQueries({ queryKey: orgKeys.list });
      return created;
    },
  });
}
