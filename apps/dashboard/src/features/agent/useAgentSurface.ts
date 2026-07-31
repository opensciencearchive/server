"use client";

import { useQuery } from "@tanstack/react-query";

import { useServices } from "@/api/services";
import type { AgentSurface } from "@/domain/agent";

/**
 * The archive's agent-grounding surface (SKILL.md + root discovery), read
 * through the same read proxy as every other archive call — self-host via
 * `/api/osa/agent/*`, platform via the control-plane read-proxy. The dashboard
 * never addresses an archive's origin directly.
 */
export function useAgentSurface(archiveId: string) {
  const { osa } = useServices();
  return useQuery<AgentSurface>({
    queryKey: ["archives", archiveId, "agent-surface"],
    queryFn: () => osa.getAgentSurface(archiveId),
  });
}
