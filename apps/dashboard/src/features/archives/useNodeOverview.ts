"use client";

import { useQuery } from "@tanstack/react-query";

import { decodeNodeOverview } from "@/api/osa/wire/decode";
import type { NodeOverview } from "@/domain/node";

/**
 * The self-hosted node's overview (identity + status + counts), fetched via the
 * BFF `/api/node` route. Drives the self-host Overview hero.
 */
export function useNodeOverview() {
  return useQuery<NodeOverview>({
    queryKey: ["node-overview"],
    queryFn: async () => {
      const res = await fetch("/api/node", {
        headers: { accept: "application/json" },
      });
      if (!res.ok) {
        throw new Error(`Node overview request failed (${res.status})`);
      }
      return decodeNodeOverview(await res.json());
    },
  });
}
