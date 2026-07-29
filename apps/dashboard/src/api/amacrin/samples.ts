/**
 * Sample data for the @mock methods on AmacrinService (no backing endpoint
 * yet). Served by BOTH implementations so "real" mode still renders the
 * full mockup — always behind the `Mocked<T>` brand. Deterministic on
 * purpose: no randomness, fixed timestamps.
 */
import type {
  BuildListItem,
  DeploymentHistoryEntry,
  OrgMember,
} from "@/domain/tenant";

export function sampleBuildList(archiveId: string): BuildListItem[] {
  void archiveId;
  return [
    {
      id: "build_8f3a91c2e5",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "4c1e77b",
      statusKind: "published",
      createdAt: new Date("2026-07-25T14:01:30Z"),
    },
    {
      id: "build_c9a8b7c6d5",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "77aa001",
      statusKind: "cancelled",
      createdAt: new Date("2026-07-25T13:59:00Z"),
    },
    {
      id: "build_f4a1e6d902",
      conventionSlug: "geo-rnaseq-v2",
      conventionRef: "b2a4f60",
      statusKind: "build_failed",
      createdAt: new Date("2026-07-25T11:40:00Z"),
    },
    {
      id: "build_0ld3rbu1ld",
      conventionSlug: "geo-rnaseq-v1",
      conventionRef: "19c3d02",
      statusKind: "published",
      createdAt: new Date("2026-07-18T09:12:00Z"),
    },
  ];
}

export function sampleDeploymentHistory(
  archiveId: string,
): DeploymentHistoryEntry[] {
  void archiveId;
  return [
    {
      id: "deploy_9a1b2c3d4e",
      deployedAt: new Date("2026-07-25T14:06:00Z"),
      fromRef: "geo-rnaseq-v2@4c1e77b",
      durationSeconds: 222,
      outcome: "succeeded",
    },
    {
      id: "deploy_pr3v10us01",
      deployedAt: new Date("2026-07-18T09:20:00Z"),
      fromRef: "geo-rnaseq-v1@19c3d02",
      durationSeconds: 251,
      outcome: "succeeded",
    },
    {
      id: "deploy_f1rstb00t0",
      deployedAt: new Date("2026-07-14T10:06:30Z"),
      fromRef: "geo-rnaseq-v1@0b442e1",
      durationSeconds: 264,
      outcome: "succeeded",
    },
  ];
}

export function sampleOrgMembers(orgId: string): OrgMember[] {
  void orgId;
  return [
    {
      name: "Rut Bergström",
      email: "r.bergstrom@example.ac.uk",
      role: "admin",
      joinedAt: new Date("2026-05-02T09:20:00Z"),
    },
    {
      name: "Priya Marsh",
      email: "p.marsh@example.ac.uk",
      role: "owner",
      joinedAt: new Date("2026-04-28T11:00:00Z"),
    },
    {
      name: "Tomás Oliveira",
      email: "t.oliveira@example.ac.uk",
      role: "member",
      joinedAt: new Date("2026-06-10T15:45:00Z"),
    },
  ];
}
