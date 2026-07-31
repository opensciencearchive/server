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
 * This collapses concurrent refreshes of the same token, within a process, into
 * one call whose result every caller shares — restoring the single-flight
 * guarantee the retired client-side `SessionRefresher` provided. It is per-pod
 * (module state), which covers the dominant case: a browser's parallel requests
 * on one keep-alive connection land on one pod. Cross-pod races remain bounded
 * by the control plane's grace window (no revocation).
 */

import { refreshTokens, type AmacrinTokens } from "./amacrin";

const inFlight = new Map<string, Promise<AmacrinTokens>>();

/**
 * Refresh `refreshToken`, coalescing concurrent calls for the same token into a
 * single upstream request. The entry is cleared once settled so the next
 * generation refreshes afresh.
 */
export function coalesceRefresh(refreshToken: string): Promise<AmacrinTokens> {
  const existing = inFlight.get(refreshToken);
  if (existing !== undefined) return existing;

  const pending = refreshTokens(refreshToken).finally(() => {
    inFlight.delete(refreshToken);
  });
  inFlight.set(refreshToken, pending);
  return pending;
}
