/**
 * Search namespace for the OSA SDK.
 */

import type { HttpClient } from './http';
import type {
  SearchOptions,
  IndexListResponse,
  SearchResponse,
  RecordResponse,
} from '@/types';
import { DEFAULT_INDEX, DEFAULT_LIMIT } from '@/lib/utils/constants';

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

export interface SearchInterface {
  /** List available search indexes. */
  listIndexes(): Promise<IndexListResponse>;

  /** Search records by natural language query. */
  query(
    text: string,
    indexName?: string,
    options?: SearchOptions,
  ): Promise<SearchResponse>;

  /** Fetch a single record by SRN. */
  getRecord(srn: string): Promise<RecordResponse>;
}

// ---------------------------------------------------------------------------
// Real HTTP implementation
// ---------------------------------------------------------------------------

export class SearchNamespace implements SearchInterface {
  constructor(private http: HttpClient) {}

  /** List available search indexes. */
  async listIndexes(): Promise<IndexListResponse> {
    const response = await this.http.fetch('/search/', {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });

    if (!response.ok) {
      throw await response.json();
    }

    return response.json();
  }

  /** Search records by natural language query. */
  async query(
    text: string,
    indexName: string = DEFAULT_INDEX,
    options: SearchOptions = {},
  ): Promise<SearchResponse> {
    const { offset = 0, limit = DEFAULT_LIMIT, filters = {} } = options;

    const params = new URLSearchParams({
      q: text,
      offset: offset.toString(),
      limit: limit.toString(),
    });

    for (const [key, value] of Object.entries(filters)) {
      params.append(`filter:${key}`, value);
    }

    const response = await this.http.fetch(
      `/search/${indexName}?${params.toString()}`,
      { method: 'GET', headers: { 'Content-Type': 'application/json' } },
    );

    if (!response.ok) {
      throw await response.json();
    }

    return response.json();
  }

  /** Fetch a single record by SRN. */
  async getRecord(srn: string): Promise<RecordResponse> {
    const response = await this.http.fetch(
      `/records/${encodeURIComponent(srn)}`,
      { method: 'GET', headers: { 'Content-Type': 'application/json' } },
    );

    if (!response.ok) {
      throw await response.json();
    }

    return response.json();
  }
}
