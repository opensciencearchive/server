/**
 * One deployment attempt of an archive. `url`/`completedAt` are only
 * meaningful on the settled variants; `failed` always carries a message
 * (the decoder supplies a fallback if the wire omits one).
 */
export type DeploymentStatus =
  | { kind: "pending" }
  | { kind: "in_progress" }
  | { kind: "succeeded"; url: string | null; completedAt: Date | null }
  | { kind: "failed"; errorMessage: string; completedAt: Date | null };

export interface Deployment {
  id: string;
  archiveId: string;
  provider: "aws_eks";
  status: DeploymentStatus;
  startedAt: Date;
}

export function isDeploymentSettled(status: DeploymentStatus): boolean {
  return status.kind === "succeeded" || status.kind === "failed";
}

/** Wall-clock duration of a settled deployment, in milliseconds. */
export function deploymentDurationMs(deployment: Deployment): number | null {
  const s = deployment.status;
  if ((s.kind === "succeeded" || s.kind === "failed") && s.completedAt) {
    return s.completedAt.getTime() - deployment.startedAt.getTime();
  }
  return null;
}
