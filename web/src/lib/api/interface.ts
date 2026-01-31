/**
 * API Interface Contract
 * Defines the contract that both MockAPI and OSAApi must implement.
 */

import type {
  SearchOptions,
  IndexListResponse,
  SearchResponse,
  RecordResponse,
} from '@/types';

/**
 * API interface that all implementations must satisfy.
 *
 * Implementations:
 * - MockAPI: In-memory implementation returning deterministic dummy data
 * - OSAApi: HTTP client communicating with the real OSA backend
 */
export interface ApiInterface {
  /**
   * List available search indexes.
   * @returns List of index names (e.g., ["vector"])
   */
  listIndexes(): Promise<IndexListResponse>;

  /**
   * Search records using semantic similarity.
   * @param query - Natural language search query
   * @param indexName - Index to search (default: "vector")
   * @param options - Search options (pagination, filters)
   * @returns Search results ranked by score
   */
  search(
    query: string,
    indexName?: string,
    options?: SearchOptions
  ): Promise<SearchResponse>;

  /**
   * Get a single record by SRN.
   * @param srn - Structured Resource Name
   * @returns Record details
   */
  getRecord(srn: string): Promise<RecordResponse>;
}
