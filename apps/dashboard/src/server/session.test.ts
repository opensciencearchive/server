// @vitest-environment node
// jose's webapi build needs node's global Uint8Array; jsdom's cross-realm copy
// trips its instanceof checks. This is a test-env artifact — jose runs fine in
// the Next edge/node runtime.
import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
  HANDOFF_PURPOSE,
  createSessionValue,
  readSession,
  verifyHandoffProof,
} from "./session";

const SECRET = "session-secret-value-at-least-32-chars!";

/** Mint a raw HS256 JWT the way the CLI's `_mint_handoff_token` does. */
function mintProof(payload: Record<string, unknown>, secret: string): string {
  const b64 = (s: string) => Buffer.from(s).toString("base64url");
  const header = b64(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64(JSON.stringify(payload));
  const sig = createHmac("sha256", secret)
    .update(`${header}.${body}`)
    .digest("base64url");
  return `${header}.${body}.${sig}`;
}

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

describe("verifyHandoffProof", () => {
  const now = () => Math.floor(Date.now() / 1000);

  it("accepts a valid, unexpired cli-handoff proof", async () => {
    const proof = mintProof(
      { purpose: HANDOFF_PURPOSE, iat: now(), exp: now() + 60 },
      SECRET,
    );
    expect(await verifyHandoffProof(proof, SECRET)).toBe(true);
  });

  it("rejects the wrong purpose", async () => {
    const proof = mintProof(
      { purpose: "nope", iat: now(), exp: now() + 60 },
      SECRET,
    );
    expect(await verifyHandoffProof(proof, SECRET)).toBe(false);
  });

  it("rejects a proof signed with a different secret", async () => {
    const proof = mintProof(
      { purpose: HANDOFF_PURPOSE, iat: now(), exp: now() + 60 },
      SECRET,
    );
    expect(
      await verifyHandoffProof(proof, "other-secret-value-32-chars-minimum!"),
    ).toBe(false);
  });

  it("rejects an expired proof and garbage", async () => {
    const expired = mintProof(
      { purpose: HANDOFF_PURPOSE, iat: now() - 120, exp: now() - 60 },
      SECRET,
    );
    expect(await verifyHandoffProof(expired, SECRET)).toBe(false);
    expect(await verifyHandoffProof("garbage", SECRET)).toBe(false);
  });
});
