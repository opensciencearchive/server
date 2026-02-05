/**
 * SDK types for OSA authentication.
 */

/** User information from the server */
export interface User {
  id: string;
  displayName: string | null;
  orcidId: string;
}

/** Token pair from authentication */
export interface TokenPair {
  accessToken: string;
  refreshToken: string;
  expiresAt: number; // Unix timestamp (milliseconds)
}

/** Authentication state */
export interface AuthState {
  user: User | null;
  tokens: TokenPair | null;
  isAuthenticated: boolean;
  isLoading: boolean;
}

/** Token response from server */
export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
  expires_in: number; // seconds
}

/** User response from /auth/me */
export interface UserResponse {
  id: string;
  display_name: string | null;
  orcid_id: string;
}

/** Error response from server */
export interface ErrorResponse {
  code: string;
  message: string;
}

/** Authentication callback parameters (from URL hash) */
export interface AuthCallbackParams {
  accessToken: string;
  refreshToken: string;
  tokenType: string;
  expiresIn: number;
  userId: string;
  displayName: string;
  orcidId: string;
}

/** SDK configuration options */
export interface SDKConfig {
  baseUrl: string;
  autoRefresh?: boolean;
  refreshThreshold?: number; // Refresh when this many seconds remain
}
