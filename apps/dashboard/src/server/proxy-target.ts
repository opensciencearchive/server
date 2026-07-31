/**
 * Path-safety for the BFF read proxy (issue #173). The proxy carries a
 * SUPERADMIN token, so it must only ever reach a fixed read allowlist — never
 * an arbitrary `/api/v1` route via `..` traversal.
 */

/** Read roots the proxy may forward GET requests to. */
export const ALLOWED_ROOTS = new Set([
  "stats",
  "data",
  "schemas",
  "hooks",
  "ingesters",
  "ingestions",
  "ready",
  "auth",
  "agent",
]);

/**
 * The agent-grounding docs are public, unversioned and live at the archive
 * ROOT (`GET /SKILL.md`, `GET /`), so they can't hang off `/api/v1` like every
 * other surface. They are reached through two exact aliases — the same ones
 * the cloud read-proxy accepts (`read_proxy.rs`), so the dashboard issues one
 * request shape whichever proxy is in front of it.
 */
const ROOT_ALIASES: Record<string, string> = {
  "agent/skill": "/SKILL.md",
  "agent/discovery": "/",
};

/**
 * `auth` is a single non-secret endpoint, not a subtree: only `/auth/config`
 * (provider + ORCID client id + admins; never the secret) is proxiable — never
 * the OAuth flow routes (login/callback/token/refresh/me). Mirrors the exact
 * pin in the cloud read-proxy (`read_proxy.rs`).
 */
const EXACT_ONLY: Record<string, string> = {
  auth: "/api/v1/auth/config",
};

/**
 * Resolve `path` to an upstream URL under `base`, or return null if it isn't an
 * allowed read. Both the first segment AND the NORMALIZED pathname are checked:
 * `path.join("/")` can hold `..` segments that URL normalization collapses
 * (`stats/../users` → `/api/v1/users`), so validating the normalized pathname
 * blocks traversal however it was encoded.
 */
export function resolveAllowedTarget(path: string[], base: string): URL | null {
  const root = path[0];
  if (root === undefined || !ALLOWED_ROOTS.has(root)) return null;

  if (root === "agent") {
    const alias = ROOT_ALIASES[path.join("/")];
    return alias === undefined ? null : new URL(alias, base);
  }

  const target = new URL(`/api/v1/${path.join("/")}`, base);

  const exact = EXACT_ONLY[root];
  if (exact !== undefined) {
    return target.pathname === exact ? target : null;
  }

  const allowedPrefix = `/api/v1/${root}`;
  if (
    target.pathname !== allowedPrefix &&
    !target.pathname.startsWith(`${allowedPrefix}/`)
  ) {
    return null;
  }
  return target;
}
