/**
 * OSA SDK client.
 */

import { AuthClient, parseAuthCallback } from './auth';
import { LocalTokenStorage, type TokenStorage } from './storage';
import type { SDKConfig, TokenPair, User, UserResponse } from './types';

/** Main OSA client class */
export class OSAClient {
  private config: SDKConfig;
  private storage: TokenStorage;
  private authClient: AuthClient;

  constructor(options: {
    baseUrl: string;
    storage?: TokenStorage;
    autoRefresh?: boolean;
    refreshThreshold?: number;
  }) {
    this.config = {
      baseUrl: options.baseUrl,
      autoRefresh: options.autoRefresh ?? true,
      refreshThreshold: options.refreshThreshold ?? 300,
    };

    this.storage = options.storage ?? new LocalTokenStorage();
    this.authClient = new AuthClient(this.config, this.storage);
  }

  // === Auth Methods ===

  /** Get the login URL */
  getLoginUrl(redirectUri?: string): string {
    return this.authClient.getLoginUrl(redirectUri);
  }

  /** Handle OAuth callback from URL hash */
  handleAuthCallback(hash: string): { user: User; tokens: TokenPair } | null {
    const params = parseAuthCallback(hash);
    if (!params) return null;
    return this.authClient.handleCallback(params);
  }

  /** Refresh the access token */
  async refreshToken(): Promise<TokenPair> {
    return this.authClient.refresh();
  }

  /** Logout the user */
  async logout(): Promise<void> {
    return this.authClient.logout();
  }

  /** Get stored user and tokens */
  getStoredAuth(): { user: User; tokens: TokenPair } | null {
    return this.authClient.getStoredAuth();
  }

  /** Check if user is authenticated */
  isAuthenticated(): boolean {
    const auth = this.getStoredAuth();
    return auth !== null && auth.tokens.expiresAt > Date.now();
  }

  // === API Methods ===

  /** Make an authenticated fetch request */
  async fetch(path: string, options: RequestInit = {}): Promise<Response> {
    const stored = this.storage.get();
    const headers = new Headers(options.headers);

    if (stored?.tokens?.accessToken) {
      headers.set('Authorization', `Bearer ${stored.tokens.accessToken}`);
    }

    const response = await fetch(`${this.config.baseUrl}${path}`, {
      ...options,
      headers,
    });

    // If 401 and we have a refresh token, try to refresh and retry
    if (response.status === 401 && stored?.tokens?.refreshToken) {
      try {
        await this.refreshToken();
        const newStored = this.storage.get();
        if (newStored?.tokens?.accessToken) {
          headers.set('Authorization', `Bearer ${newStored.tokens.accessToken}`);
          return fetch(`${this.config.baseUrl}${path}`, {
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

  /** Get current user info from server */
  async getUser(): Promise<User | null> {
    const response = await this.fetch('/auth/me');
    if (!response.ok) {
      return null;
    }

    const data = (await response.json()) as UserResponse;
    return {
      id: data.id,
      displayName: data.display_name,
      orcidId: data.orcid_id,
    };
  }
}
