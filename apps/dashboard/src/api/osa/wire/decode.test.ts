import { describe, expect, it } from "vitest";

import { decodeHookCatalog } from "./decode";

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
