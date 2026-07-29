"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";

import { requirePlatform, useServices } from "@/api/services";

export function useSignOut() {
  const services = useServices();
  const queryClient = useQueryClient();
  const router = useRouter();

  return useMutation({
    // Platform: end the cloud session. Self-host: clear the BFF session cookie.
    mutationFn: async () => {
      if (services.isPlatform) {
        await requirePlatform(services).amacrin.logout();
        return;
      }
      await fetch("/api/auth/logout", { method: "POST" });
    },
    // Local teardown happens even if the network call failed — logout is
    // idempotent server-side and the user asked to leave.
    onSettled: () => {
      if (services.isPlatform) requirePlatform(services).tokenStore.clear();
      queryClient.clear();
      router.replace("/sign-in");
    },
  });
}
