/** Environment access shared by server and client code. */

export type ApiMode = "real" | "mock" | "msw";

export function apiModeFromEnv(): ApiMode {
  const mode = process.env.NEXT_PUBLIC_API_MODE;
  if (mode === "mock" || mode === "msw") return mode;
  return "real";
}

/**
 * Runtime identity of this build (issue #173).
 *
 * - `true` (hosted) — the fleet/provisioning surface: orgs, archive lifecycle,
 *   builds. Auth goes through the Amacrin control plane.
 * - `false` (self-hosted, the default) — one archive (this server). Project-level
 *   pages only, collapsed to a single-archive root; data comes from the local
 *   archive via the same-origin BFF proxy; login is a dashboard credential.
 *
 * Build-time inlined (like `NEXT_PUBLIC_API_MODE`) — a given image's identity is
 * fixed at build.
 */
export function isPlatformFromEnv(): boolean {
  return process.env.NEXT_PUBLIC_IS_PLATFORM === "true";
}

/**
 * Fixed archive id in self-host: there is exactly one archive (this server), so
 * project routes live at `/archives/local/...` with this constant.
 */
export const SELF_HOST_ARCHIVE_ID = "local";

/**
 * Same-origin base for authenticated archive reads in self-host. The browser
 * never talks to the archive directly; it goes through the BFF proxy route
 * (`src/app/api/osa/[...path]/route.ts`), which attaches the server-held token.
 */
export function apiProxyBaseUrl(): string {
  return "/api/osa";
}
