import type { ArchiveStatus } from "@/domain/archive";

/**
 * Why redeploy/destroy is unavailable mid-transition — the two blocking
 * states from `isDeployBlocked`, phrased for the affected control.
 */
export function blockedReason(status: ArchiveStatus): string | undefined {
  if (status.kind === "deploying") return "A deployment is already in progress.";
  if (status.kind === "destroying") return "This archive is being destroyed.";
  return undefined;
}
