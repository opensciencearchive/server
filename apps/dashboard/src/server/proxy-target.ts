/**
 * Path-safety for the BFF read proxy (issue #173). The proxy carries a
 * SUPERADMIN token, so it must only ever reach a fixed read allowlist — never
 * an arbitrary `/api/v1` route via `..` traversal.
 */

/** Read roots the proxy may forward GET requests to. */
export const ALLOWED_ROOTS = new Set(["stats", "data", "schemas", "hooks"]);

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

  const target = new URL(`/api/v1/${path.join("/")}`, base);
  const allowedPrefix = `/api/v1/${root}`;
  if (
    target.pathname !== allowedPrefix &&
    !target.pathname.startsWith(`${allowedPrefix}/`)
  ) {
    return null;
  }
  return target;
}
