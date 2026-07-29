import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import { mintLocalAdminToken } from "./mint-token";

function decodePayload(token: string): Record<string, unknown> {
  const payload = token.split(".")[1] ?? "";
  return JSON.parse(Buffer.from(payload, "base64url").toString());
}

describe("mintLocalAdminToken", () => {
  it("produces a three-part HS256 JWT with the seeded-admin claims", () => {
    const token = mintLocalAdminToken("secret");
    const parts = token.split(".");
    expect(parts).toHaveLength(3);

    const header = JSON.parse(Buffer.from(parts[0]!, "base64url").toString());
    expect(header).toEqual({ alg: "HS256", typ: "JWT" });

    const payload = decodePayload(token);
    // Must match the seeded admin (seed_dev_admin.py / CLI _mint_dev_token).
    expect(payload["sub"]).toBe("00000000-0000-7000-8000-0000000000a1");
    expect(payload["provider"]).toBe("local");
    expect(payload["external_id"]).toBe("admin@osa.local");
    expect(payload["aud"]).toBe("authenticated");
    expect(payload["exp"]).toBeGreaterThan(payload["iat"] as number);
  });

  it("signs HS256 over the shared secret, so the server can verify it", () => {
    const secret = "shared-secret-value-32-chars-minimum!!";
    const token = mintLocalAdminToken(secret);
    const [h, p, sig] = token.split(".");
    const expected = createHmac("sha256", secret)
      .update(`${h}.${p}`)
      .digest("base64url");
    expect(sig).toBe(expected);
  });

  it("mints a fresh jti each call", () => {
    const a = decodePayload(mintLocalAdminToken("s"));
    const b = decodePayload(mintLocalAdminToken("s"));
    expect(a["jti"]).not.toBe(b["jti"]);
  });
});
