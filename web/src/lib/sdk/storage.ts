/**
 * Token storage for authentication.
 */

import type { TokenPair, User } from './types';

const STORAGE_KEY = 'osa_auth';

interface StoredAuth {
  tokens: TokenPair;
  user: User;
}

/** Interface for token storage implementations */
export interface TokenStorage {
  get(): StoredAuth | null;
  set(auth: StoredAuth): void;
  clear(): void;
}

/** LocalStorage implementation of TokenStorage */
export class LocalTokenStorage implements TokenStorage {
  get(): StoredAuth | null {
    if (typeof window === 'undefined') {
      return null;
    }

    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (!stored) return null;

      const auth = JSON.parse(stored) as StoredAuth;

      // Validate the stored data has required fields
      if (!auth.tokens?.accessToken || !auth.tokens?.refreshToken || !auth.user?.id) {
        return null;
      }

      return auth;
    } catch {
      return null;
    }
  }

  set(auth: StoredAuth): void {
    if (typeof window === 'undefined') {
      return;
    }

    localStorage.setItem(STORAGE_KEY, JSON.stringify(auth));
  }

  clear(): void {
    if (typeof window === 'undefined') {
      return;
    }

    localStorage.removeItem(STORAGE_KEY);
  }
}

/** In-memory storage for SSR/testing */
export class MemoryTokenStorage implements TokenStorage {
  private auth: StoredAuth | null = null;

  get(): StoredAuth | null {
    return this.auth;
  }

  set(auth: StoredAuth): void {
    this.auth = auth;
  }

  clear(): void {
    this.auth = null;
  }
}
