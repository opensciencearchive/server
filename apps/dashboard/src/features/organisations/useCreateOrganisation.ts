"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import type { Organisation } from "@/domain/organisation";

import { authKeys } from "../auth/keys";
import { orgKeys } from "./keys";

export function useCreateOrganisation() {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();

  return useMutation({
    // Org permissions ride the access token — without a refresh the new
    // organisation 404s until the old token expires. The BFF holds the token, so
    // ask it to rotate+re-seal server-side. This is part of the mutation, not
    // just `onSuccess`: `fetch` resolves on a non-2xx, so a failed refresh must
    // be checked and surfaced — otherwise `onSuccess` would refetch with stale
    // claims and the new org would 404 while the mutation reports success.
    mutationFn: async ({ name }: { name: string }): Promise<Organisation> => {
      const created = await amacrin.createOrganisation(name);
      const refreshed = await fetch("/api/auth/refresh", { method: "POST" });
      if (!refreshed.ok) {
        throw new Error(
          "Created the organisation, but refreshing your session failed — reload to continue.",
        );
      }
      return created;
    },
    // Runs only when the refresh above succeeded, so the refetch sees the new
    // org claims. Order matters: session before org list.
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: authKeys.session });
      await queryClient.invalidateQueries({ queryKey: orgKeys.list });
    },
  });
}
