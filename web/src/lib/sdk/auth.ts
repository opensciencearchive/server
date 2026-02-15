/**
 * Authentication namespace for the OSA SDK.
 */

import type { HttpClient } from './http';
import type { TokenStorage } from './storage';
import type {
  AuthCallbackParams,
  SDKConfig,
  TokenPair,
  TokenResponse,
  User,
  UserResponse,
} from './types';

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

export interface AuthInterface {
  /** Get the login URL to redirect users to. */
  getLoginUrl(redirectUri?: string): string;

  /** Parse an OAuth callback hash and store the resulting tokens. */
  handleCallback(hash: string): { user: User; tokens: TokenPair } | null;

  /** Refresh the access token using the stored refresh token. */
  refreshToken(): Promise<TokenPair>;

  /** Logout the user (server + local). */
  logout(): Promise<void>;

  /** Retrieve stored user + tokens (null if expired or missing). */
  getStoredAuth(): { user: User; tokens: TokenPair } | null;

  /** Check whether the user has a valid, non-expired session. */
  isAuthenticated(): boolean;

  /** Fetch the current user from the server. */
  getUser(): Promise<User | null>;
}

// ---------------------------------------------------------------------------
// Standalone utility
// ---------------------------------------------------------------------------

/** Parse auth parameters from a URL hash fragment. */
export function parseAuthCallback(hash: string): AuthCallbackParams | null {
  const hashContent = hash.startsWith('#') ? hash.slice(1) : hash;

  if (!hashContent.startsWith('auth=')) {
    return null;
  }

  const paramsStr = hashContent.slice(5);

  try {
    const params = new URLSearchParams(paramsStr);

    const accessToken = params.get('access_token');
    const refreshToken = params.get('refresh_token');
    const tokenType = params.get('token_type');
    const expiresIn = params.get('expires_in');
    const userId = params.get('user_id');
    const displayName = params.get('display_name');
    const externalId = params.get('external_id');

    if (!accessToken || !refreshToken || !expiresIn || !userId || !externalId) {
      return null;
    }

    return {
      accessToken,
      refreshToken,
      tokenType: tokenType || 'Bearer',
      expiresIn: parseInt(expiresIn, 10),
      userId,
      displayName: displayName || '',
      externalId,
    };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Internal auth client (encapsulated inside AuthNamespace)
// ---------------------------------------------------------------------------

class AuthClient {
  private config: SDKConfig;
  private storage: TokenStorage;
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(config: SDKConfig, storage: TokenStorage) {
    this.config = config;
    this.storage = storage;
  }

  getLoginUrl(redirectUri?: string, provider: string = 'orcid'): string {
    const base = this.config.baseUrl.startsWith('http')
      ? this.config.baseUrl
      : `${window.location.origin}${this.config.baseUrl}`;
    const url = new URL(`${base}/auth/login`);
    url.searchParams.set('provider', provider);
    if (redirectUri) {
      url.searchParams.set('redirect_uri', redirectUri);
    }
    return url.toString();
  }

  handleCallback(params: AuthCallbackParams): { user: User; tokens: TokenPair } {
    const tokens: TokenPair = {
      accessToken: params.accessToken,
      refreshToken: params.refreshToken,
      expiresAt: Date.now() + params.expiresIn * 1000,
    };

    const user: User = {
      id: params.userId,
      displayName: params.displayName || null,
      externalId: params.externalId,
    };

    this.storage.set({ tokens, user });

    if (this.config.autoRefresh !== false) {
      this.setupAutoRefresh(tokens);
    }

    return { user, tokens };
  }

  async refresh(): Promise<TokenPair> {
    const stored = this.storage.get();
    if (!stored?.tokens?.refreshToken) {
      throw new Error('No refresh token available');
    }

    const response = await fetch(`${this.config.baseUrl}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: stored.tokens.refreshToken }),
    });

    if (!response.ok) {
      this.storage.clear();
      throw new Error('Token refresh failed');
    }

    const data = (await response.json()) as TokenResponse;

    const tokens: TokenPair = {
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
      expiresAt: Date.now() + data.expires_in * 1000,
    };

    this.storage.set({ tokens, user: stored.user });

    if (this.config.autoRefresh !== false) {
      this.setupAutoRefresh(tokens);
    }

    return tokens;
  }

  async logout(): Promise<void> {
    const stored = this.storage.get();
    if (stored?.tokens?.refreshToken) {
      try {
        await fetch(`${this.config.baseUrl}/auth/logout`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refresh_token: stored.tokens.refreshToken }),
        });
      } catch {
        // Ignore — we clear local state regardless
      }
    }

    this.storage.clear();
    this.cancelAutoRefresh();
  }

  getStoredAuth(): { user: User; tokens: TokenPair } | null {
    const stored = this.storage.get();
    if (!stored) return null;

    if (stored.tokens.expiresAt <= Date.now()) {
      return null;
    }

    return stored;
  }

  private setupAutoRefresh(tokens: TokenPair): void {
    this.cancelAutoRefresh();

    const threshold = (this.config.refreshThreshold || 300) * 1000;
    const timeUntilRefresh = tokens.expiresAt - Date.now() - threshold;

    if (timeUntilRefresh > 0) {
      this.refreshTimer = setTimeout(async () => {
        try {
          await this.refresh();
        } catch {
          // Refresh failed — user must re-authenticate
        }
      }, timeUntilRefresh);
    }
  }

  private cancelAutoRefresh(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
  }
}

// ---------------------------------------------------------------------------
// AuthNamespace — public facade
// ---------------------------------------------------------------------------

export class AuthNamespace implements AuthInterface {
  private client: AuthClient;
  private http: HttpClient;

  constructor(http: HttpClient, storage: TokenStorage, clientBaseUrl: string) {
    this.http = http;
    // Auth endpoints (login redirect, refresh, logout) are always browser-facing.
    // Use the client-facing URL — never the internal Docker URL that
    // HttpClient.baseUrl may resolve to during SSR.
    this.client = new AuthClient(
      { baseUrl: clientBaseUrl, autoRefresh: true, refreshThreshold: 300 },
      storage,
    );
  }

  /** Get the login URL to redirect users to. */
  getLoginUrl(redirectUri?: string): string {
    return this.client.getLoginUrl(redirectUri);
  }

  /** Parse an OAuth callback hash and store the resulting tokens. */
  handleCallback(hash: string): { user: User; tokens: TokenPair } | null {
    const params = parseAuthCallback(hash);
    if (!params) return null;
    return this.client.handleCallback(params);
  }

  /** Refresh the access token using the stored refresh token. */
  async refreshToken(): Promise<TokenPair> {
    return this.client.refresh();
  }

  /** Logout the user (server + local). */
  async logout(): Promise<void> {
    return this.client.logout();
  }

  /** Retrieve stored user + tokens (null if expired or missing). */
  getStoredAuth(): { user: User; tokens: TokenPair } | null {
    return this.client.getStoredAuth();
  }

  /** Check whether the user has a valid, non-expired session. */
  isAuthenticated(): boolean {
    const auth = this.getStoredAuth();
    return auth !== null && auth.tokens.expiresAt > Date.now();
  }

  /** Fetch the current user from the server. */
  async getUser(): Promise<User | null> {
    const response = await this.http.fetch('/auth/me');
    if (!response.ok) return null;

    const data = (await response.json()) as UserResponse;
    return {
      id: data.id,
      displayName: data.display_name,
      externalId: data.external_id,
    };
  }
}
