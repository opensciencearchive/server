/**
 * Server-only client for the Amacrin control plane's token endpoints (#185).
 *
 * Used by the platform BFF auth routes to exchange a one-time handoff code for
 * a token pair, rotate it, and revoke it — all server-to-server against the
 * in-cluster {@link amacrinApiUrl}, never from the browser. The wire contract
 * mirrors `osa-cloud-server` `routes/auth.rs`: `POST /auth/token {code}` and
 * `POST /auth/refresh {refresh_token}` return
 * `{access_token, refresh_token, expires_in}`; `POST /auth/logout` is idempotent.
 */

import { z } from "zod";

import { amacrinApiUrl } from "./env";

/** A control-plane token pair as the BFF holds it (sealed into the session). */
export interface AmacrinTokens {
  accessToken: string;
  refreshToken: string;
  expiresInSeconds: number;
}

const tokenResponse = z.object({
  access_token: z.string(),
  expires_in: z.number(),
  refresh_token: z.string(),
});

async function postJson(path: string, body: unknown): Promise<Response> {
  return fetch(`${amacrinApiUrl()}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function parseTokens(response: Response, context: string): Promise<AmacrinTokens> {
  if (!response.ok) {
    throw new Error(`${context} failed (${response.status})`);
  }
  const parsed = tokenResponse.safeParse(await response.json());
  if (!parsed.success) {
    throw new Error(`${context}: malformed token response`);
  }
  return {
    accessToken: parsed.data.access_token,
    refreshToken: parsed.data.refresh_token,
    expiresInSeconds: parsed.data.expires_in,
  };
}

/** Exchange a one-time handoff code (from the OAuth callback) for a token pair. */
export async function exchangeHandoffCode(code: string): Promise<AmacrinTokens> {
  return parseTokens(await postJson("/api/v1/auth/token", { code }), "handoff exchange");
}

/** Rotate a token pair via the JSON-body refresh flow (the cloud rotates the refresh token). */
export async function refreshTokens(refreshToken: string): Promise<AmacrinTokens> {
  return parseTokens(
    await postJson("/api/v1/auth/refresh", { refresh_token: refreshToken }),
    "token refresh",
  );
}

/** Revoke the session server-side. Idempotent — a dead credential still resolves. */
export async function revokeSession(refreshToken: string): Promise<void> {
  const response = await postJson("/api/v1/auth/logout", { refresh_token: refreshToken });
  if (!response.ok && response.status !== 401) {
    throw new Error(`logout failed (${response.status})`);
  }
}
