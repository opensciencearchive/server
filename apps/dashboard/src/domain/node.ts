/**
 * A self-hosted node's overview — identity + live status, assembled from the
 * archive's public root discovery (`GET /`), readiness (`GET /api/v1/ready`),
 * and stats (`GET /api/v1/stats`). Powers the self-host Overview hero.
 */
export type NodeStatus = "ready" | "degraded" | "unknown";

export interface NodeOverview {
  name: string;
  domain: string;
  description: string;
  osaVersion: string;
  status: NodeStatus;
  /** Published record count, or null when stats are unavailable. */
  records: number | null;
  schemas: number;
}
