/**
 * API Factory
 * Exports the active API implementation based on environment configuration.
 */

import type { ApiInterface } from './interface';
import { MockAPI } from './mock';
import { OSAApi } from './osa';
import { API_MODE } from '@/lib/utils/constants';

/**
 * Get the API implementation based on environment.
 */
function createApi(): ApiInterface {
  if (API_MODE === 'mock') {
    return new MockAPI();
  }
  return new OSAApi();
}

/** Active API instance */
export const api = createApi();

// Re-export types for convenience
export type { ApiInterface } from './interface';
