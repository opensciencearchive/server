'use client';

/* eslint-disable react-hooks/set-state-in-effect */
import { createContext, useCallback, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { osa, type AuthState, type TokenPair, type User } from '@/lib/sdk';

interface AuthContextValue extends AuthState {
  login: () => void;
  logout: () => Promise<void>;
  refreshAuth: () => Promise<void>;
}

export const AuthContext = createContext<AuthContextValue | null>(null);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [tokens, setTokens] = useState<TokenPair | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  // Initialize auth state from storage (client-side only)
  useEffect(() => {
    // Check for OAuth callback in URL hash first
    const hash = window.location.hash;
    if (hash && hash.includes('auth=')) {
      const auth = osa.auth.handleCallback(hash);
      if (auth) {
        setUser(auth.user);
        setTokens(auth.tokens);
        window.history.replaceState(null, '', window.location.pathname + window.location.search);
      }
    } else {
      // Load from storage
      const stored = osa.auth.getStoredAuth();
      if (stored) {
        setUser(stored.user);
        setTokens(stored.tokens);
      }
    }
    setIsLoading(false);
  }, []);

  const login = useCallback(() => {
    const loginUrl = osa.auth.getLoginUrl(window.location.href);
    window.location.href = loginUrl;
  }, []);

  const logout = useCallback(async () => {
    await osa.auth.logout();
    setUser(null);
    setTokens(null);
  }, []);

  const refreshAuth = useCallback(async () => {
    try {
      const newTokens = await osa.auth.refreshToken();
      setTokens(newTokens);
    } catch {
      setUser(null);
      setTokens(null);
    }
  }, []);

  const isAuthenticated = user !== null && tokens !== null;

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
