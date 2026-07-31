"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";

export function useSignOut() {
  const queryClient = useQueryClient();
  const router = useRouter();

  return useMutation({
    // Both builds clear the same-origin BFF session cookie. Platform also
    // revokes the control-plane session server-side (#185); self-host just drops
    // the local mint. The browser holds no token either way.
    mutationFn: async () => {
      await fetch("/api/auth/logout", { method: "POST" });
    },
    // Local teardown happens even if the network call failed — logout is
    // idempotent server-side and the user asked to leave.
    onSettled: () => {
      queryClient.clear();
      router.replace("/sign-in");
    },
  });
}
