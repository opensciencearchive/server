import { createHmac, randomBytes } from "node:crypto";

/**
 * Mint an HS256 archive token for the seeded local SUPERADMIN.
 *
 * A faithful port of the CLI's `_mint_dev_token`
 * (`sdk/py/osa/cli/instance.py`): same claims, same algorithm. The OSA server —
 * sharing `JWT_SECRET` — validates the signature, treats `sub` as an internal
 * user id, and resolves the `admin@osa.local` row seeded as SUPERADMIN. The
 * dashboard holds this token server-side only (inside the session cookie).
 *
 * Node runtime only (`node:crypto`); never import from edge middleware.
 */

/** Fixed seeded-admin id — matches `seed_dev_admin.py` / the CLI. */
const LOCAL_ADMIN_USER_ID = "00000000-0000-7000-8000-0000000000a1";
const TOKEN_TTL_SECONDS = 86400 * 365;

function b64url(input: Buffer | string): string {
  return Buffer.from(input).toString("base64url");
}

export function mintLocalAdminToken(secret: string): string {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = b64url(
    JSON.stringify({
      sub: LOCAL_ADMIN_USER_ID,
      provider: "local",
      external_id: "admin@osa.local",
      aud: "authenticated",
      iat: now,
      exp: now + TOKEN_TTL_SECONDS,
      jti: randomBytes(16).toString("hex"),
    }),
  );
  const message = `${header}.${payload}`;
  const signature = b64url(createHmac("sha256", secret).update(message).digest());
  return `${message}.${signature}`;
}
