export const buildKeys = {
  detail: (buildId: string) => ["builds", buildId] as const,
  list: (archiveId: string) => ["archives", archiveId, "builds"] as const,
};
