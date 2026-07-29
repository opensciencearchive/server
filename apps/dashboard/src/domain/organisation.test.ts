import { describe, expect, it } from "vitest";

import { type Role, roleAtLeast } from "./organisation";

describe("roleAtLeast (Member < Admin < Owner)", () => {
  const cases: Array<[Role, Role, boolean]> = [
    ["member", "member", true],
    ["member", "admin", false],
    ["member", "owner", false],
    ["admin", "member", true],
    ["admin", "admin", true],
    ["admin", "owner", false],
    ["owner", "member", true],
    ["owner", "admin", true],
    ["owner", "owner", true],
  ];

  it.each(cases)("%s ≥ %s → %s", (have, need, expected) => {
    expect(roleAtLeast(have, need)).toBe(expected);
  });
});
