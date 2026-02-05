'use client';

import { useContext } from 'react';
import { AuthContext } from '@/components/auth/AuthProvider';

/**
 * Hook to access authentication state and actions.
 *
 * @example
 * ```tsx
 * function MyComponent() {
 *   const { user, isAuthenticated, login, logout } = useAuth();
 *
 *   if (!isAuthenticated) {
 *     return <button onClick={login}>Sign in</button>;
 *   }
 *
 *   return (
 *     <div>
 *       <span>Hello, {user?.displayName}!</span>
 *       <button onClick={logout}>Sign out</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useAuth() {
  const context = useContext(AuthContext);

  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }

  return context;
}
