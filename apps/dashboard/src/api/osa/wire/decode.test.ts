import { describe, expect, it } from "vitest";

import { decodeAgentSurface, decodeHookCatalog } from "./decode";

describe("decodeHookCatalog", () => {
  it("maps a hook with a live release", () => {
    const out = decodeHookCatalog({
      items: [
        {
          name: "validate-metadata",
          feature: { name: "validation", columns: [{ name: "a" }, { name: "b" }] },
          live_release: {
            version: 3,
            digest: "sha256:abc",
            source_ref: "git@sha",
            built_at: "2026-07-25T16:20:00Z",
          },
        },
      ],
    });

    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({
      name: "validate-metadata",
      liveVersion: "v3",
    });
    expect(out[0]!.description).toContain("validation");
    expect(out[0]!.description).toContain("2 columns");
    expect(out[0]!.lastRunAt).toEqual(new Date("2026-07-25T16:20:00Z"));
  });

  it("handles a hook with no live release", () => {
    const out = decodeHookCatalog({
      items: [{ name: "x", feature: { name: "f" }, live_release: null }],
    });
    expect(out[0]!.liveVersion).toBe("—");
    expect(out[0]!.lastRunAt).toBeNull();
  });

  it("rejects a malformed payload", () => {
    expect(() => decodeHookCatalog({ items: [{ name: 123 }] })).toThrow();
  });
});

describe("decodeAgentSurface", () => {
  const discovery = {
    node: {
      name: "My Archive",
      domain: "archive.example.org",
      description: "d",
      osa_version: "0.0.7",
    },
    skill_url: "https://archive.example.org/SKILL.md",
    reference_base: "https://archive.example.org/api/v1/data",
    data_url: "https://archive.example.org/api/v1/data",
    openapi_url: "https://archive.example.org/api/v1/openapi.json",
  };

  it("maps skill + node and derives the /mcp url from the origin", () => {
    const out = decodeAgentSurface({ skill: "# Hello", discovery });
    expect(out.skillMarkdown).toBe("# Hello");
    expect(out.node.name).toBe("My Archive");
    expect(out.node.osaVersion).toBe("0.0.7");
    expect(out.mcpUrl).toBe("https://archive.example.org/mcp");
  });

  it("falls back to /mcp when skill_url is relative", () => {
    const out = decodeAgentSurface({
      skill: "x",
      discovery: { ...discovery, skill_url: "/SKILL.md" },
    });
    expect(out.mcpUrl).toBe("/mcp");
  });
});
