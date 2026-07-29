export const buildKeys = {
  detail: (buildId: string) => ["builds", buildId] as const,
  /** @mock — no list endpoint; served from Mocked sample data. */
  list: (archiveId: string) => ["archives", archiveId, "builds"] as const,
};
