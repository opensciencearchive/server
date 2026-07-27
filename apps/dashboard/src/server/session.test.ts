// @vitest-environment node
// jose's webapi build needs node's global Uint8Array; jsdom's cross-realm copy
// trips its instanceof checks. This is a test-env artifact — jose runs fine in
// the Next edge/node runtime.
import { describe, expect, it } from "vitest";

import { createSessionValue, readSession } from "./session";

const SECRET = "session-secret-value-at-least-32-chars!";

describe("session cookie", () => {
  it("round-trips a session envelope", async () => {
    const value = await createSessionValue(
      { sub: "admin@osa.local", osaToken: "the-archive-token" },
      SECRET,
    );
    expect(await readSession(value, SECRET)).toEqual({
      sub: "admin@osa.local",
      osaToken: "the-archive-token",
    });
  });

  it("returns null for an absent or malformed cookie", async () => {
    expect(await readSession(undefined, SECRET)).toBeNull();
    expect(await readSession("", SECRET)).toBeNull();
    expect(await readSession("not-a-jwt", SECRET)).toBeNull();
  });

  it("rejects a cookie signed with a different secret", async () => {
    const value = await createSessionValue(
      { sub: "x", osaToken: "t" },
      SECRET,
    );
    expect(await readSession(value, "some-other-secret-value-32-chars!!!")).toBeNull();
  });
});
