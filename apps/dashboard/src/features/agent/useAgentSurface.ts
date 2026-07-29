"use client";

import { useQuery } from "@tanstack/react-query";

import { decodeAgentSurface } from "@/api/osa/wire/decode";
import type { AgentSurface } from "@/domain/agent";

/**
 * The archive's agent-grounding surface (SKILL.md + root discovery), fetched via
 * the BFF `/api/agent` route. Real in self-host; in a platform build there is no
 * local archive to reach, so the query errors and the page shows an empty state.
 */
export function useAgentSurface() {
  return useQuery<AgentSurface>({
    queryKey: ["agent-surface"],
    queryFn: async () => {
      const res = await fetch("/api/agent", {
        headers: { accept: "application/json" },
      });
      if (!res.ok) {
        throw new Error(`Agent surface request failed (${res.status})`);
      }
      return decodeAgentSurface(await res.json());
    },
  });
}
