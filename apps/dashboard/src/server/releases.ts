/**
 * Cached GitHub release notes for the upgrade dialog (#222).
 *
 * Proxied server-side because GitHub's unauthenticated API is rate-limited
 * per IP — one cached fetch here instead of one per browser.
 */

import { nextPageUrl } from "./link-header";

const REPO = "opensciencearchive/server";
const CACHE_TTL_MS = 10 * 60 * 1000;
// 10 × 50 releases ≫ any plausible upgrade range; the cap only guards
// against a pathological Link-header loop.
const MAX_PAGES = 10;

export const RELEASE_TAG = /^v\d+\.\d+\.\d+$/;

export interface Release {
  version: string;
  name: string;
  body: string;
  htmlUrl: string;
}

let cached: { releases: Release[]; at: number } | null = null;

/** Test hook: drop the module cache. */
export function resetReleasesCache(): void {
  cached = null;
}

/** All published (non-draft, non-prerelease) releases. Throws on GitHub failure. */
export async function publishedReleases(): Promise<Release[]> {
  if (cached && Date.now() - cached.at < CACHE_TTL_MS) return cached.releases;
  const releases: Release[] = [];
  let url: string | null =
    `https://api.github.com/repos/${REPO}/releases?per_page=50`;
  for (let page = 0; url !== null && page < MAX_PAGES; page++) {
    const res: Response = await fetch(url, {
      headers: { accept: "application/vnd.github+json" },
    });
    if (!res.ok) throw new Error(`github releases: ${res.status}`);
    const body = (await res.json()) as {
      tag_name: string;
      name: string | null;
      body: string | null;
      html_url: string;
      draft: boolean;
      prerelease: boolean;
    }[];
    releases.push(
      ...body
        .filter(
          (r) => !r.draft && !r.prerelease && RELEASE_TAG.test(r.tag_name),
        )
        .map((r) => ({
          version: r.tag_name,
          name: r.name ?? r.tag_name,
          body: r.body ?? "",
          htmlUrl: r.html_url,
        })),
    );
    url = nextPageUrl(res.headers.get("link"), "https://api.github.com");
  }
  cached = { releases, at: Date.now() };
  return releases;
}
