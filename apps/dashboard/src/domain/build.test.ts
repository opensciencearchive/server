import { describe, expect, it } from "vitest";

import { type BuildStatus, isBuildTerminal } from "./build";

describe("isBuildTerminal", () => {
  const cases: Array<[BuildStatus, boolean]> = [
    [{ kind: "queued" }, false],
    [{ kind: "building" }, false],
    [{ kind: "publishing" }, false],
    [{ kind: "published", publishedAt: new Date() }, true],
    [{ kind: "build_failed", errorMessage: "x" }, true],
    [{ kind: "publish_failed", errorMessage: "x" }, true],
    [{ kind: "cancelled", cancelledBy: "user", cancelReason: null }, true],
  ];

  it.each(cases)("%o → %s", (status, expected) => {
    expect(isBuildTerminal(status)).toBe(expected);
  });
});
