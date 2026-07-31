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

/**
 * The dashboard's public origin as the browser sees it — `scheme://host[:port]`.
 * Behind an ingress the direct bind (`0.0.0.0:3000`) is not the address the
 * browser used, so honour `X-Forwarded-Proto`/`X-Forwarded-Host` first. Used to
 * build the OAuth `redirect_uri` the control plane bounces back to (#185), which
 * must match the registered dashboard origin exactly.
 */
export function requestOrigin(req: NextRequest): string {
  const proto =
    req.headers.get("x-forwarded-proto")?.split(",")[0]?.trim() ??
    req.nextUrl.protocol.replace(/:$/, "");
  const host =
    req.headers.get("x-forwarded-host") ?? req.headers.get("host") ?? req.nextUrl.host;
  return `${proto}://${host}`;
}
