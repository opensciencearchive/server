/**
 * Latest published OSA release, straight from the container registry.
 *
 * The registry (not GitHub releases) is the availability signal: a tag in
 * ghcr is pullable by definition. Same flow as the SDK's
 * `fetch_latest_osa_version` — anonymous pull-scope token, paginated
 * tags/list, highest strict `vX.Y.Z` client-side. Cached in-module (~1h);
 * `null` on any failure so callers degrade instead of breaking.
 */

import { nextPageUrl } from "./link-header";

const GHCR_IMAGE = "opensciencearchive/osa";
const CACHE_TTL_MS = 60 * 60 * 1000;
const MAX_PAGES = 50;
// One deadline for the whole lookup: /api/node awaits this, and the version
// hint is optional — a slow registry must degrade to null, not stall the
// overview behind its loading skeleton.
const DEADLINE_MS = 4_000;
const RELEASE_TAG = /^v(\d+)\.(\d+)\.(\d+)$/;

let cached: { value: string | null; at: number } | null = null;

export async function latestOsaVersion(): Promise<string | null> {
  if (cached && Date.now() - cached.at < CACHE_TTL_MS) return cached.value;
  const value = await fetchLatest().catch(() => null);
  // Cache failures too — a down registry shouldn't be re-probed per render.
  cached = { value, at: Date.now() };
  return value;
}

/** Test hook: drop the module cache. */
export function resetLatestOsaVersionCache(): void {
  cached = null;
}

async function fetchLatest(): Promise<string | null> {
  const signal = AbortSignal.timeout(DEADLINE_MS);
  const tokenRes = await fetch(
    `https://ghcr.io/token?scope=repository:${GHCR_IMAGE}:pull`,
    { signal },
  );
  if (!tokenRes.ok) return null;
  const token = (await tokenRes.json()).token as string | undefined;
  if (!token) return null;

  const tags: string[] = [];
  let url: string | null = `https://ghcr.io/v2/${GHCR_IMAGE}/tags/list?n=1000`;
  for (let page = 0; url !== null && page < MAX_PAGES; page++) {
    const res: Response = await fetch(url, {
      headers: { authorization: `Bearer ${token}` },
      signal,
    });
    if (!res.ok) return null;
    const body = (await res.json()) as { tags?: string[] };
    tags.push(...(body.tags ?? []));
    url = nextPageUrl(res.headers.get("link"), "https://ghcr.io");
  }

  const releases = tags
    .map((t) => ({ tag: t, m: RELEASE_TAG.exec(t) }))
    .filter((x): x is { tag: string; m: RegExpExecArray } => x.m !== null)
    .sort(
      (a, b) =>
        Number(a.m[1]) - Number(b.m[1]) ||
        Number(a.m[2]) - Number(b.m[2]) ||
        Number(a.m[3]) - Number(b.m[3]),
    );
  return releases.at(-1)?.tag ?? null;
}
