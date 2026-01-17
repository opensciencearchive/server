/**
 * Application constants
 */

/** Default search index name */
export const DEFAULT_INDEX = 'vector';

/** Default pagination limit */
export const DEFAULT_LIMIT = 20;

/** Maximum pagination limit */
export const MAX_LIMIT = 100;

/** API base URL for production */
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'https://api.opensciencearchive.org/api/v1';

/** API mode: 'mock' or 'live' */
export const API_MODE = process.env.NEXT_PUBLIC_API_MODE || 'mock';
