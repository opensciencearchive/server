export const orgKeys = {
  list: ["organisations"] as const,
  detail: (orgId: string) => ["organisations", orgId] as const,
  archives: (orgId: string) => ["organisations", orgId, "archives"] as const,
  members: (orgId: string) => ["organisations", orgId, "members"] as const,
};
