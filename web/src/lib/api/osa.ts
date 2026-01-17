/**
 * OSAApi Implementation
 * HTTP client for the real OSA backend.
 */

import type { ApiInterface } from './interface';
import type {
  SearchOptions,
  IndexListResponse,
  SearchResponse,
  RecordResponse,
} from '@/types';
import { API_BASE_URL, DEFAULT_INDEX, DEFAULT_LIMIT } from '@/lib/utils/constants';

/**
 * OSAApi - HTTP client for the Open Science Archive API.
 */
export class OSAApi implements ApiInterface {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  async listIndexes(): Promise<IndexListResponse> {
    const response = await fetch(`${this.baseUrl}/search/`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      const error = await response.json();
      throw error;
    }

    return response.json();
  }

  async search(
    query: string,
    indexName: string = DEFAULT_INDEX,
    options: SearchOptions = {}
  ): Promise<SearchResponse> {
    const { offset = 0, limit = DEFAULT_LIMIT, filters = {} } = options;

    // Build query parameters
    const params = new URLSearchParams({
      q: query,
      offset: offset.toString(),
      limit: limit.toString(),
    });

    // Add filter parameters
    for (const [key, value] of Object.entries(filters)) {
      params.append(`filter:${key}`, value);
    }

    const response = await fetch(
      `${this.baseUrl}/search/${indexName}?${params.toString()}`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );

    if (!response.ok) {
      const error = await response.json();
      throw error;
    }

    return response.json();
  }

  async getRecord(srn: string): Promise<RecordResponse> {
    const response = await fetch(
      `${this.baseUrl}/records/${encodeURIComponent(srn)}`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );

    if (!response.ok) {
      const error = await response.json();
      throw error;
    }

    return response.json();
  }
}
