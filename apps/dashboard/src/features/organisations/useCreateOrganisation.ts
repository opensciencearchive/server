"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import type { Organisation } from "@/domain/organisation";

import { authKeys } from "../auth/keys";
import { orgKeys } from "./keys";

/** Outcome of creating an organisation. `created` is always committed on success. */
export interface CreateOrganisationResult {
  created: Organisation;
  /**
   * Whether the follow-up session refresh succeeded. Org permissions ride the
   * access token, so a fresh token is required to *see* the new org. When false,
   * the session is effectively dead and the caller should re-authenticate (the
   * org exists and surfaces after sign-in) rather than navigate to a 404.
   */
  sessionRefreshed: boolean;
}

export function useCreateOrganisation() {
  const { amacrin } = usePlatformServices();
  const queryClient = useQueryClient();

  return useMutation({
    // The create is the mutation's success — it commits once, so a failed
    // *refresh* must never surface as a mutation error (that would re-enable the
    // form and let the user create a duplicate). We report the refresh outcome
    // instead and let the caller decide: navigate to the org, or re-auth.
    mutationFn: async ({ name }: { name: string }): Promise<CreateOrganisationResult> => {
      const created = await amacrin.createOrganisation(name);
      // The BFF holds the token; ask it to rotate+re-seal so the new org claim
      // is present. The refresh must NEVER fail the mutation (the create already
      // committed): a non-2xx OR a transport rejection both resolve to
      // `sessionRefreshed: false`, and the caller re-auths instead of retrying.
      let sessionRefreshed = false;
      try {
        const refreshed = await fetch("/api/auth/refresh", { method: "POST" });
        sessionRefreshed = refreshed.ok;
      } catch {
        sessionRefreshed = false;
      }
      return { created, sessionRefreshed };
    },
    // Only refetch with the new claims when the refresh actually landed —
    // otherwise the queries would repopulate from the stale token. Order
    // matters: session before org list.
    onSuccess: async ({ sessionRefreshed }) => {
      if (!sessionRefreshed) return;
      await queryClient.invalidateQueries({ queryKey: authKeys.session });
      await queryClient.invalidateQueries({ queryKey: orgKeys.list });
    },
  });
}
