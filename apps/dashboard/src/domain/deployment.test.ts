import { describe, expect, it } from "vitest";

import { type DeploymentStatus, isDeploymentSettled } from "./deployment";

describe("isDeploymentSettled", () => {
  const cases: Array<[DeploymentStatus, boolean]> = [
    [{ kind: "pending" }, false],
    [{ kind: "in_progress" }, false],
    [{ kind: "succeeded", url: "https://x.amacr.in", completedAt: null }, true],
    [{ kind: "failed", errorMessage: "boom", completedAt: null }, true],
  ];

  it.each(cases)("%o → %s", (status, expected) => {
    expect(isDeploymentSettled(status)).toBe(expected);
  });
});
