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
 * Same-origin JSON client for the platform BFF (issue #185).
 *
 * The browser holds no bearer token — it calls the dashboard's own
 * `/api/amacrin/*` proxy, which attaches the sealed access token server-side
 * and transparently refreshes it. So this client just sends same-origin
 * cookie-authenticated requests. A `401` means the whole session is gone (the
 * server-side refresh already failed): it fires `onUnauthorized` (sign-out +
 * redirect) and surfaces the ApiError. Error bodies are decoded to `ApiError`,
 * carrying the `x-request-id` for supportability.
 */
export class HttpClient {
  private readonly baseUrl: string;
  private readonly onUnauthorized: (() => void) | undefined;

  constructor(args: { baseUrl: string; onUnauthorized?: () => void }) {
    this.baseUrl = args.baseUrl.replace(/\/$/, "");
    this.onUnauthorized = args.onUnauthorized;
  }

  get(path: string): Promise<unknown> {
    return this.request(path, { method: "GET" });
  }

  post(path: string, body?: unknown): Promise<unknown> {
    return this.request(path, { method: "POST", body });
  }

  private async request(path: string, options: RequestOptions): Promise<unknown> {
    const response = await this.send(path, options);
    if (response.status === 401) {
      this.onUnauthorized?.();
      throw await apiErrorFromResponse(response);
    }
    return this.settle(response);
  }

  private async send(path: string, options: RequestOptions): Promise<Response> {
    const headers = new Headers();
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
