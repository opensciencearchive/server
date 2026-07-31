import { SignJWT, jwtVerify } from "jose";

/**
 * The platform (hosted) dashboard session cookie (issue #185).
 *
 * Option-2 session-bearing BFF: the browser talks only to the dashboard origin
 * and never holds a control-plane token. The BFF keeps the user's access +
 * refresh tokens **sealed inside the session cookie** — a signed (HS256 over
 * `SESSION_SECRET`) envelope, httpOnly — and unwraps them server-side to attach
 * a bearer to proxied control-plane calls (`/api/amacrin/*`) and to refresh.
 *
 * This mirrors the self-host `osa_session` envelope in `session.ts`, but seals
 * a `{access, refresh}` pair instead of a single minted archive token. Signing
 * is `jose`, so `middleware.ts` (edge runtime) can verify it too. The cookie
 * name is shared (`SESSION_COOKIE`) — a build is either platform or self-host,
 * never both, so the shapes never collide.
 */

import { SESSION_COOKIE, sessionCookieOptions } from "./session";

export { SESSION_COOKIE, sessionCookieOptions };

/** How long a sealed platform session is valid before a fresh sign-in. */
const PLATFORM_SESSION_TTL_SECONDS = 60 * 60 * 24 * 30; // 30d — bounded by refresh-token life

export interface PlatformSessionData {
  /** Control-plane access token — attached as the bearer by the data proxy. */
  accessToken: string;
  /** Refresh token — used server-side to rotate the access token on 401. */
  refreshToken: string;
}

function key(secret: string): Uint8Array {
  return new TextEncoder().encode(secret);
}

/**
 * Sign a platform session envelope for `Set-Cookie`. Only the token pair is
 * sealed — the user's identity is read authoritatively from `/auth/me` through
 * the proxy, so it is not duplicated here.
 */
export async function createPlatformSessionValue(
  data: PlatformSessionData,
  secret: string,
): Promise<string> {
  return new SignJWT({ act: data.accessToken, rt: data.refreshToken })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(`${PLATFORM_SESSION_TTL_SECONDS}s`)
    .sign(key(secret));
}

/** Verify a cookie value; returns the session, or null if absent/invalid/expired. */
export async function readPlatformSession(
  value: string | undefined,
  secret: string,
): Promise<PlatformSessionData | null> {
  if (value === undefined || value === "") return null;
  try {
    const { payload } = await jwtVerify(value, key(secret));
    const accessToken = payload["act"];
    const refreshToken = payload["rt"];
    if (typeof accessToken === "string" && typeof refreshToken === "string") {
      return { accessToken, refreshToken };
    }
    return null;
  } catch {
    // Invalid signature / malformed / expired — treat as no session.
    return null;
  }
}

/**
 * Sign-in CSRF nonce (issue #185).
 *
 * Login sets this in a short-lived cookie and round-trips the same value through
 * the control plane via the `redirect_uri` query; the callback rejects the
 * handoff code unless the returned nonce matches the cookie. This binds the
 * completing browser to the one that started the flow — without it, an attacker
 * could feed a victim a callback URL carrying the attacker's handoff code and
 * silently sign the victim into the attacker's account (login CSRF).
 */
export const SIGNIN_NONCE_COOKIE = "osa_signin_nonce";
const NONCE_TTL_SECONDS = 600; // 10 minutes — one OAuth round trip

/** Cookie attributes for the sign-in nonce. `Secure` is caller-decided. */
export function nonceCookieOptions(secure: boolean) {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure,
    path: "/",
    maxAge: NONCE_TTL_SECONDS,
  };
}

/** Cookie attributes that clear the sign-in nonce (consumed at callback). */
export function clearNonceCookieOptions(secure: boolean) {
  return { ...nonceCookieOptions(secure), maxAge: 0 };
}
