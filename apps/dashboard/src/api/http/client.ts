import type { SessionRefresher } from "@/api/auth/refresher";

import { ApiError, TransportError } from "./errors";

interface RequestOptions {
  method: "GET" | "POST" | "DELETE";
  body?: unknown;
}

/** Decode a non-2xx response body (`{error, message, reason?, request_id?}`). */
export async function apiErrorFromResponse(
  response: Response,
): Promise<ApiError> {
  const requestId = response.headers.get("x-request-id");
  let code = "unknown";
  let message = `request failed with status ${response.status}`;
  let reason: string | null = null;
  let bodyRequestId: string | null = null;
  try {
    const body = (await response.json()) as Record<string, unknown>;
    if (typeof body["error"] === "string") code = body["error"];
    if (typeof body["message"] === "string") message = body["message"];
    if (typeof body["reason"] === "string") reason = body["reason"];
    if (typeof body["request_id"] === "string") {
      bodyRequestId = body["request_id"];
    }
  } catch {
    // Non-JSON error body — keep the status-derived defaults.
  }
  return new ApiError({
    status: response.status,
    code,
    message,
    reason,
    requestId: requestId ?? bodyRequestId,
  });
}

/**
 * Authenticated JSON client for the Amacrin Cloud API.
 *
 * Every request carries the in-memory bearer token (proactively refreshed
 * near expiry). On a 401 the client refreshes once and retries once; a
 * second 401 (or a dead refresh) surfaces as the original ApiError — the
 * refresher has already fired `onSessionLost`. Error bodies are decoded to
 * `ApiError`, carrying the `x-request-id` for supportability.
 */
export class HttpClient {
  private readonly baseUrl: string;
  private readonly refresher: SessionRefresher;

  constructor(args: { baseUrl: string; refresher: SessionRefresher }) {
    this.baseUrl = args.baseUrl.replace(/\/$/, "");
    this.refresher = args.refresher;
  }

  get(path: string): Promise<unknown> {
    return this.request(path, { method: "GET" });
  }

  post(path: string, body?: unknown): Promise<unknown> {
    return this.request(path, { method: "POST", body });
  }

  private async request(path: string, options: RequestOptions): Promise<unknown> {
    const token = await this.refresher.ensureFreshToken();
    const response = await this.send(path, options, token);

    if (response.status !== 401) return this.settle(response);

    // One refresh, one retry — never a loop. A null token means the
    // session is gone; surface the original 401.
    const fresh = await this.refresher.forceRefresh();
    if (fresh === null) throw await apiErrorFromResponse(response);
    return this.settle(await this.send(path, options, fresh));
  }

  private async send(
    path: string,
    options: RequestOptions,
    token: string | null,
  ): Promise<Response> {
    const headers = new Headers();
    if (token !== null) headers.set("authorization", `Bearer ${token}`);
    if (options.body !== undefined) {
      headers.set("content-type", "application/json");
    }

    try {
      return await fetch(`${this.baseUrl}${path}`, {
        method: options.method,
        headers,
        body:
          options.body === undefined ? undefined : JSON.stringify(options.body),
      });
    } catch (cause) {
      throw new TransportError(`request to ${path} failed`, { cause });
    }
  }

  private async settle(response: Response): Promise<unknown> {
    if (!response.ok) throw await apiErrorFromResponse(response);
    if (response.status === 204) return undefined;
    try {
      return await response.json();
    } catch (cause) {
      throw new TransportError("response was not valid JSON", { cause });
    }
  }
}
