/**
 * API types for Lingual Bio Search
 * Aligned with OSA Search API v1.0.0
 */

import type { Record as OSARecord, SearchHit } from './record';

/**
 * Options for semantic search queries.
 */
export interface SearchOptions {
  /** Number of results to skip (default: 0) */
  offset?: number;

  /** Maximum results to return (default: 20, max: 100) */
  limit?: number;

  /** Metadata filters using "filter:<path>=<value>" format */
  filters?: { [key: string]: string };
}

/**
 * Response from listing search indexes.
 */
export interface IndexListResponse {
  /** Names of available search indexes */
  indexes: string[];
}

/**
 * Response from a semantic search query.
 */
export interface SearchResponse {
  /** The query that was executed */
  query: string;

  /** Name of the index that was searched */
  index: string;

  /** Total number of matching records (not just returned) */
  total: number;

  /** Whether more results are available beyond current page */
  has_more: boolean;

  /** Array of search hits */
  results: SearchHit[];
}

/**
 * Response from fetching a single record.
 */
export interface RecordResponse {
  /** The requested record */
  record: OSARecord;
}

/**
 * API error response.
 */
export interface ApiError {
  /** Human-readable error message */
  detail: string;
}

/**
 * Type guard to check if an error is an ApiError.
 */
export function isApiError(error: unknown): error is ApiError {
  return (
    typeof error === 'object' &&
    error !== null &&
    'detail' in error &&
    typeof (error as ApiError).detail === 'string'
  );
}

/**
 * Result type for operations that can fail.
 */
export type ApiResult<T> =
  | { success: true; data: T }
  | { success: false; error: ApiError };
