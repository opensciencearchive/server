import type { NextRequest } from "next/server";

/**
 * Same-origin check for state-changing POSTs (login/logout). A cross-site form
 * post carries a foreign `Origin`; a same-origin one either matches `Host` or
 * (some browsers) omits `Origin` entirely, which we allow. Combined with the
 * session cookie's `SameSite=Lax`, this is adequate CSRF defence for the BFF.
 */
export function isSameOrigin(req: NextRequest): boolean {
  const origin = req.headers.get("origin");
  if (origin === null) return true;
  try {
    return new URL(origin).host === req.headers.get("host");
  } catch {
    return false;
  }
}

/**
 * Whether the request arrived over HTTPS — gates the cookie `Secure` flag so a
 * plain-HTTP LAN self-host doesn't silently drop the session cookie. Honours the
 * proxy's `X-Forwarded-Proto` ahead of the (often internal) direct protocol.
 */
export function isSecureRequest(req: NextRequest): boolean {
  const forwarded = req.headers.get("x-forwarded-proto");
  if (forwarded !== null) return forwarded.split(",")[0]?.trim() === "https";
  return req.nextUrl.protocol === "https:";
}
