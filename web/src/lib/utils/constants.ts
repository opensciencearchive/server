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

/** API mode: 'mock' or 'live' */
export const API_MODE = process.env.NEXT_PUBLIC_API_MODE || 'live';
