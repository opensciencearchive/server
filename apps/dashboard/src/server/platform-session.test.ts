// @vitest-environment node
// jose's webapi build needs node's global Uint8Array; jsdom's cross-realm copy
// trips its instanceof checks (see session.test.ts).
import { describe, expect, it } from "vitest";

import {
  createPlatformSessionValue,
  readPlatformSession,
} from "./platform-session";

const SECRET = "session-secret-value-at-least-32-chars!";
const OTHER = "a-different-secret-at-least-32-chars-long!";

describe("platform session cookie", () => {
  it("round-trips a sealed token pair", async () => {
    const value = await createPlatformSessionValue(
      { accessToken: "acc.tok", refreshToken: "ref.tok" },
      SECRET,
    );
    expect(await readPlatformSession(value, SECRET)).toEqual({
      accessToken: "acc.tok",
      refreshToken: "ref.tok",
    });
  });

  it("returns null for an absent or malformed cookie", async () => {
    expect(await readPlatformSession(undefined, SECRET)).toBeNull();
    expect(await readPlatformSession("", SECRET)).toBeNull();
    expect(await readPlatformSession("not-a-jwt", SECRET)).toBeNull();
  });

  it("rejects a cookie signed with a different secret", async () => {
    const value = await createPlatformSessionValue(
      { accessToken: "acc.tok", refreshToken: "ref.tok" },
      OTHER,
    );
    expect(await readPlatformSession(value, SECRET)).toBeNull();
  });
});
