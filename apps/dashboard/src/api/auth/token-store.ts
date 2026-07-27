/**
 * In-memory access-token holder. Deliberately NOT persisted anywhere — a
 * full page load starts with an empty store and restores the session via
 * the refresh cookie (see SessionRefresher). Never touches web storage.
 */
export class TokenStore {
  private token: string | null = null;
  private expiresAt = 0;

  set(token: string, expiresInSeconds: number): void {
    this.token = token;
    this.expiresAt = Date.now() + expiresInSeconds * 1000;
  }

  get(): string | null {
    return this.token;
  }

  /** True when the token expires within `ms` — or when there is no token. */
  expiresWithin(ms: number): boolean {
    if (this.token === null) return true;
    return this.expiresAt - Date.now() < ms;
  }

  clear(): void {
    this.token = null;
    this.expiresAt = 0;
  }
}

/** The app-wide store. Tests construct their own instances. */
export const tokenStore = new TokenStore();
