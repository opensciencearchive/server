/**
 * OSA SDK - TypeScript client for Open Science Archive API.
 *
 * @example
 * ```typescript
 * import { OSAClient } from '@/lib/sdk';
 *
 * const client = new OSAClient({ baseUrl: '/api/v1' });
 *
 * // Check if user is authenticated
 * if (client.isAuthenticated()) {
 *   const user = client.getStoredAuth()?.user;
 *   console.log(`Hello, ${user?.displayName}!`);
 * }
 *
 * // Get login URL
 * const loginUrl = client.getLoginUrl();
 * window.location.href = loginUrl;
 *
 * // Handle callback (in callback page)
 * const auth = client.handleAuthCallback(window.location.hash);
 * if (auth) {
 *   console.log('Logged in as:', auth.user);
 * }
 *
 * // Logout
 * await client.logout();
 * ```
 */

export { OSAClient } from './client';
export { AuthClient, parseAuthCallback } from './auth';
export { LocalTokenStorage, MemoryTokenStorage, type TokenStorage } from './storage';
export type {
  AuthCallbackParams,
  AuthState,
  ErrorResponse,
  SDKConfig,
  TokenPair,
  TokenResponse,
  User,
  UserResponse,
} from './types';
