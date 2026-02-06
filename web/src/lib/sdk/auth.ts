/**
 * Authentication client for OSA SDK.
 */

import type { TokenStorage } from './storage';
import type {
  AuthCallbackParams,
  SDKConfig,
  TokenPair,
  TokenResponse,
  User,
} from './types';

/** Parse auth parameters from URL hash */
export function parseAuthCallback(hash: string): AuthCallbackParams | null {
  // Remove leading # if present
  const hashContent = hash.startsWith('#') ? hash.slice(1) : hash;

  // Check if it starts with "auth="
  if (!hashContent.startsWith('auth=')) {
    return null;
  }

  // Remove "auth=" prefix
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

    if (!accessToken || !refreshToken || !expiresIn || !userId || !orcidId) {
      return null;
    }

    return {
      accessToken,
      refreshToken,
      tokenType: tokenType || 'Bearer',
      expiresIn: parseInt(expiresIn, 10),
      userId,
      displayName: displayName || '',
      orcidId,
    };
  } catch {
    return null;
  }
}

/** Authentication client class */
export class AuthClient {
  private config: SDKConfig;
  private storage: TokenStorage;
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(config: SDKConfig, storage: TokenStorage) {
    this.config = config;
    this.storage = storage;
  }

  /** Get the login URL to redirect users to */
  getLoginUrl(redirectUri?: string): string {
    // Handle both relative and absolute baseUrl
    const base = this.config.baseUrl.startsWith('http')
      ? this.config.baseUrl
      : `${window.location.origin}${this.config.baseUrl}`;
    console.log('Base URL for auth:', base);
    const url = new URL(`${base}/auth/login`);
    if (redirectUri) {
      url.searchParams.set('redirect_uri', redirectUri);
    }
    return url.toString();
  }

  /** Handle OAuth callback and store tokens */
  handleCallback(params: AuthCallbackParams): { user: User; tokens: TokenPair } {
    const tokens: TokenPair = {
      accessToken: params.accessToken,
      refreshToken: params.refreshToken,
      expiresAt: Date.now() + params.expiresIn * 1000,
    };

    const user: User = {
      id: params.userId,
      displayName: params.displayName || null,
      orcidId: params.orcidId,
    };

    // Store auth data
    this.storage.set({ tokens, user });

    // Setup auto-refresh if enabled
    if (this.config.autoRefresh !== false) {
      this.setupAutoRefresh(tokens);
    }

    return { user, tokens };
  }

  /** Refresh the access token */
  async refresh(): Promise<TokenPair> {
    const stored = this.storage.get();
    if (!stored?.tokens?.refreshToken) {
      throw new Error('No refresh token available');
    }

    const response = await fetch(`${this.config.baseUrl}/auth/refresh`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        refresh_token: stored.tokens.refreshToken,
      }),
    });

    if (!response.ok) {
      // Clear storage on auth error
      this.storage.clear();
      throw new Error('Token refresh failed');
    }

    const data = (await response.json()) as TokenResponse;

    const tokens: TokenPair = {
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
      expiresAt: Date.now() + data.expires_in * 1000,
    };

    // Update storage with new tokens
    this.storage.set({ tokens, user: stored.user });

    // Setup next auto-refresh
    if (this.config.autoRefresh !== false) {
      this.setupAutoRefresh(tokens);
    }

    return tokens;
  }

  /** Logout the user */
  async logout(): Promise<void> {
    const stored = this.storage.get();
    if (stored?.tokens?.refreshToken) {
      try {
        await fetch(`${this.config.baseUrl}/auth/logout`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            refresh_token: stored.tokens.refreshToken,
          }),
        });
      } catch {
        // Ignore logout errors - we're clearing local state anyway
      }
    }

    // Clear local state
    this.storage.clear();
    this.cancelAutoRefresh();
  }

  /** Get stored auth data */
  getStoredAuth(): { user: User; tokens: TokenPair } | null {
    const stored = this.storage.get();
    if (!stored) return null;

    // Check if tokens are still valid
    if (stored.tokens.expiresAt <= Date.now()) {
      // Token expired, but we might be able to refresh
      // For now, return null and let the caller handle refresh
      return null;
    }

    return stored;
  }

  /** Setup auto-refresh timer */
  private setupAutoRefresh(tokens: TokenPair): void {
    this.cancelAutoRefresh();

    const threshold = (this.config.refreshThreshold || 300) * 1000; // Default 5 minutes
    const timeUntilRefresh = tokens.expiresAt - Date.now() - threshold;

    if (timeUntilRefresh > 0) {
      this.refreshTimer = setTimeout(async () => {
        try {
          await this.refresh();
        } catch {
          // Refresh failed, user will need to re-authenticate
        }
      }, timeUntilRefresh);
    }
  }

  /** Cancel auto-refresh timer */
  private cancelAutoRefresh(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
  }
}
