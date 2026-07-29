"use client";

import { useQuery } from "@tanstack/react-query";

import { requirePlatform, useServices } from "@/api/services";
import type { Session } from "@/domain/user";

import { authKeys } from "./keys";

/**
 * The single local operator identity in a self-host build. There is no
 * `/auth/me` — the session is a dashboard credential (M2b), and every operator
 * acts as the seeded local SUPERADMIN.
 */
const SELF_HOST_SESSION: Session = {
  user: { id: "local-admin", email: "admin@osa.local", createdAt: new Date(0) },
  organisations: [],
};

/** The signed-in user + their organisations (platform: GET /auth/me). */
export function useSession() {
  const services = useServices();
  return useQuery({
    queryKey: authKeys.session,
    queryFn: () =>
      services.isPlatform
        ? requirePlatform(services).amacrin.getMe()
        : Promise.resolve(SELF_HOST_SESSION),
    staleTime: 60_000,
  });
}
