import { SignJWT, jwtVerify } from "jose";

/**
 * The dashboard session cookie (issue #173).
 *
 * A signed (HS256 over `SESSION_SECRET`) envelope wrapping the minted archive
 * token. The envelope is what the browser holds (httpOnly); the archive token
 * stays opaque inside it and is only unwrapped server-side to attach to proxied
 * archive calls. Signing is `jose`, so `middleware.ts` (edge runtime) can verify
 * it too.
 */

export const SESSION_COOKIE = "osa_session";
const SESSION_TTL_SECONDS = 60 * 60 * 12; // 12h

export interface SessionData {
  /** Operator identity — the local admin's external id. */
  sub: string;
  /** The minted archive bearer token, forwarded by the proxy. */
  osaToken: string;
}

function key(secret: string): Uint8Array {
  return new TextEncoder().encode(secret);
}

/** Sign a session envelope for `Set-Cookie`. */
export async function createSessionValue(
  data: SessionData,
  secret: string,
): Promise<string> {
  return new SignJWT({ osaToken: data.osaToken })
    .setProtectedHeader({ alg: "HS256" })
    .setSubject(data.sub)
    .setIssuedAt()
    .setExpirationTime(`${SESSION_TTL_SECONDS}s`)
    .sign(key(secret));
}

/** Verify a cookie value; returns the session, or null if absent/invalid/expired. */
export async function readSession(
  value: string | undefined,
  secret: string,
): Promise<SessionData | null> {
  if (value === undefined || value === "") return null;
  try {
    const { payload } = await jwtVerify(value, key(secret));
    const osaToken = payload["osaToken"];
    if (typeof payload.sub === "string" && typeof osaToken === "string") {
      return { sub: payload.sub, osaToken };
    }
    return null;
  } catch {
    // Invalid signature / malformed / expired — treat as no session.
    return null;
  }
}

/** The `purpose` claim on a CLI passwordless-handoff proof (see `osa dashboard`). */
export const HANDOFF_PURPOSE = "cli-handoff";

/**
 * Verify a CLI handoff proof: a short-lived HS256 JWT signed with the shared
 * SESSION_SECRET, carrying `purpose: "cli-handoff"`. Only a holder of the secret
 * can forge one, so a valid proof authenticates the CLI. Returns false on any
 * bad signature / expiry / purpose.
 */
export async function verifyHandoffProof(
  proof: string,
  secret: string,
): Promise<boolean> {
  try {
    const { payload } = await jwtVerify(proof, key(secret), {
      algorithms: ["HS256"],
    });
    return payload["purpose"] === HANDOFF_PURPOSE;
  } catch {
    return false;
  }
}

/** Cookie attributes. `Secure` is caller-decided (see `isSecureRequest`). */
export function sessionCookieOptions(secure: boolean) {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure,
    path: "/",
    maxAge: SESSION_TTL_SECONDS,
  };
}
