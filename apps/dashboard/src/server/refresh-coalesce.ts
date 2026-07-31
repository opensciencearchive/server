/**
 * Single-flight coalescing for control-plane refresh (issue #185).
 *
 * The control plane rotates the refresh token on every use. Concurrent BFF
 * requests that all hit an expired access token would each present the SAME
 * sealed refresh token — the winner rotates it, the losers present a
 * rotated-away token. The control plane's grace window keeps that from revoking
 * the session (a lost race reads as "denied", not "reuse"), but each loser still
 * gets a 401 and the client signs the user out.
 *
 * Two layers close that window, per process:
 *  1. in-flight coalescing — concurrent refreshes of the same token share one
 *     upstream call and its result.
 *  2. a short-lived result cache — a request that arrives just AFTER the refresh
 *     settled (its browser cookie not yet updated) still carries the old token;
 *     it is handed the freshly rotated pair from cache instead of re-submitting
 *     the rotated-away token upstream (which would 401). The TTL stays within the
 *     control plane's reuse grace window.
 *
 * Both layers are per-pod (module state), covering the dominant case: a browser's
 * parallel/near-sequential requests on one keep-alive connection land on one pod.
 * Cross-pod races remain bounded by the control plane's grace window (a lost race
 * is denied, never a revocation).
 */

import { refreshTokens, type AmacrinTokens } from "./amacrin";

/** Kept within the control plane's refresh-reuse grace window. */
const RESULT_TTL_MS = 30_000;

const inFlight = new Map<string, Promise<AmacrinTokens>>();
const recent = new Map<string, { tokens: AmacrinTokens; at: number }>();

/**
 * Refresh `refreshToken`, coalescing concurrent AND just-settled calls for the
 * same token so a burst rotates it exactly once and every caller shares the
 * result. Returns the rotated pair.
 */
export function coalesceRefresh(refreshToken: string): Promise<AmacrinTokens> {
  const live = inFlight.get(refreshToken);
  if (live !== undefined) return live;

  const cached = recent.get(refreshToken);
  if (cached !== undefined) {
    if (Date.now() - cached.at < RESULT_TTL_MS) return Promise.resolve(cached.tokens);
    recent.delete(refreshToken);
  }

  const pending = refreshTokens(refreshToken)
    .then((tokens) => {
      const entry = { tokens, at: Date.now() };
      recent.set(refreshToken, entry);
      // Evict after the grace window — but only if this is still OUR entry. A
      // later refresh of the same key replaces it; without the identity check a
      // stale timer would delete that fresh result. unref so it never keeps the
      // process alive.
      setTimeout(() => {
        if (recent.get(refreshToken) === entry) recent.delete(refreshToken);
      }, RESULT_TTL_MS).unref?.();
      return tokens;
    })
    .finally(() => {
      inFlight.delete(refreshToken);
    });
  inFlight.set(refreshToken, pending);
  return pending;
}
