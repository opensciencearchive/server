export const upgradeKeys = {
  versions: ["osa-versions"] as const,
  releaseNotes: (from: string, to: string) =>
    ["release-notes", from, to] as const,
};
