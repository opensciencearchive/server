/**
 * Error surface of the Amacrin Cloud API.
 *
 * The server renders every error as
 * `{ error: string, message: string, reason?: string, request_id?: string }`
 * (see osa-cloud-server `middleware/error.rs`). `ApiError` carries that body
 * plus the HTTP status; `request_id` rides the `x-request-id` header too and
 * is surfaced for supportability.
 */
export class ApiError extends Error {
  readonly status: number;
  /** Machine-readable code, e.g. "duplicate", "not_found", "unauthorized". */
  readonly code: string;
  /** 401s carry why: "missing" | "expired" | "invalid". */
  readonly reason: string | null;
  readonly requestId: string | null;

  constructor(args: {
    status: number;
    code: string;
    message: string;
    reason?: string | null;
    requestId?: string | null;
  }) {
    super(args.message);
    this.name = "ApiError";
    this.status = args.status;
    this.code = args.code;
    this.reason = args.reason ?? null;
    this.requestId = args.requestId ?? null;
  }
}

/** A create-archive 409: the slug is already taken. */
export class SlugTakenError extends ApiError {
  readonly slug: string;

  constructor(slug: string, base: ApiError) {
    super({
      status: base.status,
      code: base.code,
      message: base.message,
      reason: base.reason,
      requestId: base.requestId,
    });
    this.name = "SlugTakenError";
    this.slug = slug;
  }
}

/** Network failure or non-JSON response — no server-rendered error body. */
export class TransportError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "TransportError";
  }
}
