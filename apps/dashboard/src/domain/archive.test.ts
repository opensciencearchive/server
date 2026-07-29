import { describe, expect, it } from "vitest";

import { type ArchiveStatus, isDeployBlocked } from "./archive";

describe("isDeployBlocked", () => {
  const cases: Array<[ArchiveStatus, boolean]> = [
    [{ kind: "deploying" }, true],
    [{ kind: "destroying" }, true],
    [{ kind: "running" }, false],
    [{ kind: "stopped" }, false],
    [{ kind: "error", message: "x" }, false],
  ];

  it.each(cases)("%o → %s", (status, expected) => {
    expect(isDeployBlocked(status)).toBe(expected);
  });
});
