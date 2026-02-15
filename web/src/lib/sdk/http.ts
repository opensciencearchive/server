/**
 * Shared HTTP client with automatic auth token injection and 401 retry.
 */

import type { TokenStorage } from './storage';

export class HttpClient {
  private refreshFn: (() => Promise<unknown>) | null = null;

  constructor(
    readonly baseUrl: string,
    private storage?: TokenStorage,
  ) {}

  /** Wire a token-refresh callback (set after construction to avoid circular deps). */
  setRefreshFn(fn: () => Promise<unknown>): void {
    this.refreshFn = fn;
  }

  /** Make an HTTP request, attaching auth headers and retrying once on 401. */
  async fetch(path: string, options: RequestInit = {}): Promise<Response> {
    const stored = this.storage?.get();
    const headers = new Headers(options.headers);

    if (stored?.tokens?.accessToken) {
      headers.set('Authorization', `Bearer ${stored.tokens.accessToken}`);
    }

    const response = await fetch(`${this.baseUrl}${path}`, {
      ...options,
      headers,
    });

    // If 401 and we have a refresh token, try to refresh and retry
    if (response.status === 401 && stored?.tokens?.refreshToken && this.refreshFn) {
      try {
        await this.refreshFn();
        const newStored = this.storage?.get();
        if (newStored?.tokens?.accessToken) {
          headers.set('Authorization', `Bearer ${newStored.tokens.accessToken}`);
          return fetch(`${this.baseUrl}${path}`, {
            ...options,
            headers,
          });
        }
      } catch {
        // Refresh failed, return original 401 response
      }
    }

    return response;
  }
}
