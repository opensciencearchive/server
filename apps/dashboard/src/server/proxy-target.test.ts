import { describe, expect, it } from "vitest";

import { resolveAllowedTarget } from "./proxy-target";

const BASE = "http://server:8000";

describe("resolveAllowedTarget", () => {
  it("allows a read on an allowlisted root", () => {
    const target = resolveAllowedTarget(["hooks"], BASE);
    expect(target?.toString()).toBe(`${BASE}/api/v1/hooks`);
  });

  it("allows nested paths under an allowed root", () => {
    const target = resolveAllowedTarget(["data", "stations", "records"], BASE);
    expect(target?.pathname).toBe("/api/v1/data/stations/records");
  });

  it("rejects a non-allowlisted root", () => {
    expect(resolveAllowedTarget(["users"], BASE)).toBeNull();
    expect(resolveAllowedTarget([], BASE)).toBeNull();
  });

  it("rejects `..` traversal that would escape the allowed root", () => {
    // The exploit Greptile found: allowed first segment, then `..` to /users.
    expect(resolveAllowedTarget(["stats", "..", "users"], BASE)).toBeNull();
    expect(
      resolveAllowedTarget(["data", "..", "..", "conventions"], BASE),
    ).toBeNull();
  });

  it("rejects an encoded-slash root that isn't an exact allowed segment", () => {
    // If the traversal arrives as a single segment, it fails the root check.
    expect(resolveAllowedTarget(["stats/../users"], BASE)).toBeNull();
  });
});
