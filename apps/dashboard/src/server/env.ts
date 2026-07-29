/**
 * Server-only environment access for the BFF (issue #173).
 *
 * Read at RUNTIME by route handlers and `middleware.ts`; never bundled into the
 * client. Secrets (`JWT_SECRET`, `SESSION_SECRET`, `DASHBOARD_PASSWORD`) live
 * here and nowhere the browser can see them. Accessors fail fast so a
 * misconfigured deployment errors loudly rather than serving an open dashboard.
 */

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

/** Base URL of the local OSA archive API (e.g. http://server:8000). */
export function osaApiUrl(): string {
  return required("OSA_API_URL").replace(/\/$/, "");
}

/** Shared HS256 secret — must equal the server's OSA_AUTH__JWT__SECRET. */
export function jwtSecret(): string {
  return required("JWT_SECRET");
}

/** Secret signing the dashboard session cookie (independent of JWT_SECRET). */
export function sessionSecret(): string {
  return required("SESSION_SECRET");
}

/** Dashboard login username. Defaults to "admin" when unset. */
export function dashboardUsername(): string {
  return process.env.DASHBOARD_USERNAME ?? "admin";
}

/** Dashboard login password. Required — no default. */
export function dashboardPassword(): string {
  return required("DASHBOARD_PASSWORD");
}
