"use client";

/**
 * Route protection.
 *
 * Both builds now hold a same-origin httpOnly session cookie that `middleware.ts`
 * verifies on every guarded request (#185, previously self-host only in #173),
 * so an unauthenticated user is redirected to `/sign-in` before any page renders
 * — there is nothing to bootstrap client-side. This component renders straight
 * through and stays as the mount point for any future client-side gating.
 */
export function AuthGuard({ children }: { children: React.ReactNode }) {
  return children;
}
