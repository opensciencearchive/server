/**
 * OSA version registry entries (#222) — what the cloud can provision.
 *
 * Registered automatically by the cloud's release poller; `status` is the
 * support lifecycle. Only `supported` versions are upgrade targets; a
 * `withdrawn` pin cannot even be redeployed.
 */

export type OsaVersionStatus = "supported" | "deprecated" | "withdrawn";

export interface OsaVersion {
  /** Strict `vX.Y.Z` tag, e.g. `v0.0.12`. */
  version: string;
  status: OsaVersionStatus;
  /** The version new archives are born with. */
  isDefault: boolean;
  /** Release notes link, stamped by the registry poller. */
  notesUrl: string | null;
}

/**
 * Numeric semver comparison, tolerant of a missing `v` prefix (the tenant
 * health endpoint reports `0.0.11` while registry tags say `v0.0.11`).
 * Returns <0 / 0 / >0 like a comparator. Unparseable versions compare as
 * lowest so they never masquerade as an upgrade.
 */
export function compareOsaVersions(a: string, b: string): number {
  const pa = parseVersion(a);
  const pb = parseVersion(b);
  if (!pa && !pb) return 0;
  if (!pa) return -1;
  if (!pb) return 1;
  for (let i = 0; i < 3; i++) {
    const d = (pa[i] ?? 0) - (pb[i] ?? 0);
    if (d !== 0) return d;
  }
  return 0;
}

function parseVersion(v: string): [number, number, number] | null {
  const m = /^v?(\d+)\.(\d+)\.(\d+)$/.exec(v.trim());
  if (!m) return null;
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

/**
 * Versions the archive may move to: `supported` and strictly newer than the
 * pin, newest first. Mirrors the cloud's `eligible_upgrade_target()` rule so
 * the dialog never offers something the API would 400.
 */
export function eligibleUpgradeTargets(
  versions: OsaVersion[],
  pin: string,
): OsaVersion[] {
  return versions
    .filter(
      (v) =>
        v.status === "supported" && compareOsaVersions(v.version, pin) > 0,
    )
    .sort((a, b) => compareOsaVersions(b.version, a.version));
}

/** GitHub compare link between two release tags. */
export function releaseCompareUrl(from: string, to: string): string {
  return `https://github.com/opensciencearchive/server/compare/${from}...${to}`;
}
