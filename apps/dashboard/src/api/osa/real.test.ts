// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";

import { RealOSAService } from "./real";

const STATS = {
  records: 10,
  records_this_month: 2,
  features_per_record: 3,
  storage_bytes: 100,
};

const AUTH_CONFIG = {
  provider: "orcid",
  client_id: "APP-1",
  admin_orcids: ["0000-0002-1825-0097"],
};

function spyFetch(payload: unknown): string[] {
  const seen: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation((url: RequestInfo | URL) => {
    seen.push(String(url));
    return Promise.resolve(Response.json(payload));
  });
  return seen;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("RealOSAService base resolution", () => {
  it("self-host ignores archiveId and reads the single-archive proxy base", async () => {
    const seen = spyFetch(STATS);
    await new RealOSAService(() => "/api/osa").getRecordStats("arch_ignored");
    expect(seen[0]).toBe("/api/osa/stats");
  });

  it("platform resolves the per-archive control-plane read-proxy base", async () => {
    const seen = spyFetch(STATS);
    const svc = new RealOSAService(
      (id) => `/api/amacrin/api/v1/archives/${id}/osa`,
    );
    await svc.getRecordStats("arch_1");
    expect(seen[0]).toBe("/api/amacrin/api/v1/archives/arch_1/osa/stats");
  });

  it("self-host reads auth config through the same-origin proxy (auth/config)", async () => {
    const seen = spyFetch(AUTH_CONFIG);
    await new RealOSAService(() => "/api/osa").getAuthConfig("arch_ignored");
    expect(seen[0]).toBe("/api/osa/auth/config");
  });

  it("platform reads auth config through the per-archive read-proxy surface", async () => {
    const seen = spyFetch(AUTH_CONFIG);
    const svc = new RealOSAService(
      (id) => `/api/amacrin/api/v1/archives/${id}/osa`,
    );
    await svc.getAuthConfig("arch_1");
    expect(seen[0]).toBe("/api/amacrin/api/v1/archives/arch_1/osa/auth/config");
  });
});

describe("RealOSAService agent surface", () => {
  const DISCOVERY = {
    node: {
      name: "Alpine",
      domain: "alpine.example.org",
      description: "d",
      osa_version: "0.0.7",
    },
    skill_url: "https://alpine.example.org/SKILL.md",
    reference_base: "https://alpine.example.org/api/v1/data",
    data_url: "https://alpine.example.org/api/v1/data",
    openapi_url: "https://alpine.example.org/api/v1/openapi.json",
  };

  /** Serve markdown for the skill alias and JSON for discovery. */
  function spyAgentFetch(): string[] {
    const seen: string[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation((url: RequestInfo | URL) => {
      const href = String(url);
      seen.push(href);
      return Promise.resolve(
        href.endsWith("/agent/skill")
          ? new Response("# Alpine\n\nGrounding doc.", {
              headers: { "content-type": "text/markdown" },
            })
          : Response.json(DISCOVERY),
      );
    });
    return seen;
  }

  it("reads both grounding docs through the proxy, never the archive directly", async () => {
    const seen = spyAgentFetch();
    const svc = new RealOSAService(
      (id) => `/api/amacrin/api/v1/archives/${id}/osa`,
    );

    const surface = await svc.getAgentSurface("arch_1");

    expect(seen.sort()).toEqual([
      "/api/amacrin/api/v1/archives/arch_1/osa/agent/discovery",
      "/api/amacrin/api/v1/archives/arch_1/osa/agent/skill",
    ]);
    // Nothing addressed the archive's own origin.
    expect(seen.some((u) => u.includes("alpine.example.org"))).toBe(false);
    expect(surface.skillMarkdown).toContain("# Alpine");
    expect(surface.node.osaVersion).toBe("0.0.7");
    expect(surface.mcpUrl).toBe("https://alpine.example.org/mcp");
  });

  it("self-host uses the same aliases against its single-archive proxy", async () => {
    const seen = spyAgentFetch();
    await new RealOSAService(() => "/api/osa").getAgentSurface("arch_ignored");
    expect(seen.sort()).toEqual([
      "/api/osa/agent/discovery",
      "/api/osa/agent/skill",
    ]);
  });
});
