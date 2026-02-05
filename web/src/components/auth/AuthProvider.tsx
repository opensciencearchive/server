'use client';

import { createContext, useCallback, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { OSAClient, type AuthState, type TokenPair, type User } from '@/lib/sdk';

interface AuthContextValue extends AuthState {
  login: () => void;
  logout: () => Promise<void>;
  refreshAuth: () => Promise<void>;
}

export const AuthContext = createContext<AuthContextValue | null>(null);

interface AuthProviderProps {
  children: ReactNode;
  baseUrl?: string;
}

export function AuthProvider({ children, baseUrl = '/api/v1' }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [tokens, setTokens] = useState<TokenPair | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  const client = useMemo(() => new OSAClient({ baseUrl }), [baseUrl]);

  // Initialize auth state from storage
  useEffect(() => {
    const stored = client.getStoredAuth();
    if (stored) {
      setUser(stored.user);
      setTokens(stored.tokens);
    }
    setIsLoading(false);
  }, [client]);

  // Handle OAuth callback if present in URL hash
  useEffect(() => {
    if (typeof window === 'undefined') return;

    const hash = window.location.hash;
    if (hash && hash.includes('auth=')) {
      const auth = client.handleAuthCallback(hash);
      if (auth) {
        setUser(auth.user);
        setTokens(auth.tokens);
        // Clear hash from URL
        window.history.replaceState(null, '', window.location.pathname + window.location.search);
      }
    }
  }, [client]);

  const login = useCallback(() => {
    const loginUrl = client.getLoginUrl(window.location.href);
    window.location.href = loginUrl;
  }, [client]);

  const logout = useCallback(async () => {
    await client.logout();
    setUser(null);
    setTokens(null);
  }, [client]);

  const refreshAuth = useCallback(async () => {
    try {
      const newTokens = await client.refreshToken();
      setTokens(newTokens);
    } catch {
      // Refresh failed, clear auth state
      setUser(null);
      setTokens(null);
    }
  }, [client]);

  const isAuthenticated = user !== null && tokens !== null && tokens.expiresAt > Date.now();

  const value: AuthContextValue = {
    user,
    tokens,
    isAuthenticated,
    isLoading,
    login,
    logout,
    refreshAuth,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
