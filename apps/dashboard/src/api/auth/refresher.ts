import { ApiError } from "@/api/http/errors";

import type { TokenStore } from "./token-store";

export interface RefreshResult {
  accessToken: string;
  expiresInSeconds: number;
}

/** Performs POST /auth/refresh (the browser attaches the httpOnly cookie). */
export type RefreshFn = () => Promise<RefreshResult>;

/** Refresh proactively when less than this much lifetime remains. */
const EXPIRY_WINDOW_MS = 60_000;

/**
 * Single-flight session refresher.
 *
 * The API rotates the refresh token on every use and revokes the whole
 * session on token reuse, so concurrent refreshes MUST collapse into one
 * request — this is correctness, not an optimisation. A 401 from the
 * refresh endpoint means the session is gone (expired/revoked): the store
 * is cleared and `onSessionLost` fires. Transport errors propagate without
 * touching the session.
 */
export class SessionRefresher {
  private readonly store: TokenStore;
  private readonly refreshFn: RefreshFn;
  private readonly onSessionLost: (() => void) | undefined;
  private inFlight: Promise<string | null> | null = null;

  constructor(args: {
    store: TokenStore;
    refreshFn: RefreshFn;
    onSessionLost?: () => void;
  }) {
    this.store = args.store;
    this.refreshFn = args.refreshFn;
    this.onSessionLost = args.onSessionLost;
  }

  /**
   * A token with comfortable lifetime left, refreshing if needed.
   * Null when the session is lost.
   */
  async ensureFreshToken(): Promise<string | null> {
    const current = this.store.get();
    if (current !== null && !this.store.expiresWithin(EXPIRY_WINDOW_MS)) {
      return current;
    }
    return this.forceRefresh();
  }

  /** Unconditional (but single-flight) refresh — used after a 401 and after org creation. */
  forceRefresh(): Promise<string | null> {
    this.inFlight ??= this.refreshFn()
      .then(({ accessToken, expiresInSeconds }) => {
        this.store.set(accessToken, expiresInSeconds);
        return accessToken;
      })
      .catch((error: unknown) => {
        if (error instanceof ApiError && error.status === 401) {
          this.store.clear();
          this.onSessionLost?.();
          return null;
        }
        throw error;
      })
      .finally(() => {
        this.inFlight = null;
      });
    return this.inFlight;
  }
}
