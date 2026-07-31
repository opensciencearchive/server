import { describe, expect, it } from "vitest";

import { config } from "./middleware";

/**
 * The matcher is the guard's perimeter: a path it matches goes through the
 * auth gate; a path it excludes is served directly. The pattern's body is a
 * plain regex group, so compiling it anchored reproduces Next's routing
 * decision for these cases.
 *
 * Regression: `public/` assets (/osa-logo.svg) were NOT excluded, so
 * the signed-out guard 307'd them to /sign-in and the sign-in page's own
 * logo rendered broken.
 */
const guard = new RegExp(`^${config.matcher[0]}$`);

describe("middleware matcher", () => {
  it("guards the app routes", () => {
    for (const path of [
      "/",
      "/organisations",
      "/organisations/org_x",
      "/archives/new",
      "/archives/arch_x/records",
    ]) {
      expect(guard.test(path), `${path} must be guarded`).toBe(true);
    }
  });

  it("leaves sign-in, API routes, Next internals, and static files reachable", () => {
    for (const path of [
      "/sign-in",
      "/api/auth/sign-in",
      "/api/amacrin/archives",
      "/_next/static/chunks/main.js",
      "/_next/image?url=x",
      "/favicon.ico",
      // public/ assets — any path with a file extension (the fix)
      "/osa-logo.svg",
      "/icon.svg",
      "/fonts/inter.woff2",
    ]) {
      expect(guard.test(path), `${path} must NOT be guarded`).toBe(false);
    }
  });
});
