/**
 * Application constants
 */

/** Default search index name */
export const DEFAULT_INDEX = 'vector';

/** Default pagination limit */
export const DEFAULT_LIMIT = 20;

/** Maximum pagination limit */
export const MAX_LIMIT = 100;

/**
 * API base URL
 * - Server-side: Uses API_URL env var (internal Docker URL like http://server:8000)
 * - Client-side: Uses relative /api/v1 (proxied by Next.js rewrites)
 */
const isServer = typeof window === 'undefined';
const serverApiUrl = process.env.API_URL ? `${process.env.API_URL}/api/v1` : null;
export const API_BASE_URL = isServer && serverApiUrl ? serverApiUrl : '/api/v1';

/**
 * Client-facing API base URL (used for browser-initiated requests like auth).
 * - Uses NEXT_PUBLIC_API_URL if set (e.g., http://127.0.0.1:8000/api/v1 for local dev)
 * - Falls back to /api/v1 (proxied by Next.js rewrites)
 *
 * This differs from API_BASE_URL which may resolve to an internal Docker hostname
 * for SSR. Auth endpoints (login redirect, refresh, logout) must always use a
 * browser-reachable URL.
 */
export const CLIENT_API_URL = process.env.NEXT_PUBLIC_API_URL || '/api/v1';

/** API mode: 'mock' or 'live' */
export const API_MODE = process.env.NEXT_PUBLIC_API_MODE || 'live';
