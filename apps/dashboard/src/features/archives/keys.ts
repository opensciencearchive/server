export const archiveKeys = {
  list: ["archives"] as const,
  detail: (archiveId: string) => ["archives", archiveId] as const,
  status: (archiveId: string) => ["archives", archiveId, "status"] as const,
  deployments: (archiveId: string) =>
    ["archives", archiveId, "deployments"] as const,
};
