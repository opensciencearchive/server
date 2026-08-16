// @vitest-environment node
import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createPlatformSessionValue } from "@/server/platform-session";
import { resetReleasesCache } from "@/server/releases";
import { SESSION_COOKIE } from "@/server/session";

import { GET } from "./route";

const SECRET = "test-session-secret-test-session-secret!";

function githubRelease(tag: string, over?: Record<string, unknown>) {
  return {
    tag_name: tag,
    name: `Release ${tag}`,
    body: `notes for ${tag}`,
    html_url: `https://github.com/opensciencearchive/server/releases/tag/${tag}`,
    draft: false,
    prerelease: false,
    ...over,
  };
}

async function request(query: string): Promise<NextRequest> {
  const cookie = await createPlatformSessionValue(
    { accessToken: "t", refreshToken: "r" },
    SECRET,
  );
  return new NextRequest(`http://dash.test/api/releases?${query}`, {
    headers: { cookie: `${SESSION_COOKIE}=${cookie}` },
  });
}

describe("GET /api/releases", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_IS_PLATFORM", "true");
    vi.stubEnv("SESSION_SECRET", SECRET);
    resetReleasesCache();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("returns releases in (from, to], oldest first, skipping drafts", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      Response.json([
        githubRelease("v0.0.12"),
        githubRelease("v0.0.11"),
        githubRelease("v0.0.10"),
        githubRelease("v0.0.13", { draft: true }),
      ]),
    );

    const res = await GET(await request("from=v0.0.10&to=v0.0.12"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { releases: { version: string }[] };
    expect(body.releases.map((r) => r.version)).toEqual([
      "v0.0.11",
      "v0.0.12",
    ]);
  });

  it("serves from cache on the second call", async () => {
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(Response.json([githubRelease("v0.0.12")]));

    await GET(await request("from=v0.0.11&to=v0.0.12"));
    await GET(await request("from=v0.0.11&to=v0.0.12"));
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("degrades to 502 when GitHub is unavailable", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("rate limited", { status: 403 }),
    );
    const res = await GET(await request("from=v0.0.10&to=v0.0.12"));
    expect(res.status).toBe(502);
  });

  it("rejects an invalid target version", async () => {
    const res = await GET(await request("to=latest"));
    expect(res.status).toBe(400);
  });

  it("requires a session", async () => {
    const res = await GET(
      new NextRequest("http://dash.test/api/releases?to=v0.0.12"),
    );
    expect(res.status).toBe(401);
  });
});
