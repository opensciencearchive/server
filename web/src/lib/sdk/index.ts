/**
 * OSA SDK — unified client for Open Science Archive.
 *
 * @example
 * ```typescript
 * import { osa } from '@/lib/sdk';
 *
 * // Auth
 * osa.auth.getLoginUrl()
 * osa.auth.isAuthenticated()
 * const user = await osa.auth.getUser()
 *
 * // Search
 * const results = await osa.search.query('alzheimer')
 * const record = await osa.search.getRecord('urn:osa:...')
 *
 * // Deposition
 * const conventions = await osa.deposition.listConventions()
 * const { srn } = await osa.deposition.create(conventionSrn)
 * await osa.deposition.uploadSpreadsheet(srn, file)
 * await osa.deposition.submit(srn)
 * ```
 */

import { HttpClient } from './http';
import { AuthNamespace, type AuthInterface } from './auth';
import { SearchNamespace, type SearchInterface } from './search';
import { DepositionNamespace, type DepositionInterface } from './deposition';
import { LocalTokenStorage, type TokenStorage } from './storage';
import { MockSearchNamespace } from './mock/search';
import { MockDepositionNamespace } from './mock/deposition';
import { API_BASE_URL, CLIENT_API_URL, API_MODE } from '@/lib/utils/constants';

// ---------------------------------------------------------------------------
// OSA facade
// ---------------------------------------------------------------------------

export class OSA {
  constructor(
    readonly auth: AuthInterface,
    readonly search: SearchInterface,
    readonly deposition: DepositionInterface,
  ) {}
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createOSA(options?: {
  baseUrl?: string;
  storage?: TokenStorage;
}): OSA {
  const baseUrl = options?.baseUrl ?? API_BASE_URL;
  const storage =
    options?.storage ??
    (typeof window !== 'undefined' ? new LocalTokenStorage() : undefined);

  const http = new HttpClient(baseUrl, storage);

  const auth = new AuthNamespace(http, storage ?? new LocalTokenStorage(), CLIENT_API_URL);
  http.setRefreshFn(() => auth.refreshToken());

  const search =
    API_MODE === 'mock' ? new MockSearchNamespace() : new SearchNamespace(http);

  const deposition =
    API_MODE === 'mock'
      ? new MockDepositionNamespace()
      : new DepositionNamespace(http);

  return new OSA(auth, search, deposition);
}

/** SDK singleton — ready to use everywhere. */
export const osa = createOSA();

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

// Standalone utility (used in auth callback page)
export { parseAuthCallback } from './auth';

// Namespace interfaces (for typing, mocking, testing)
export type { AuthInterface } from './auth';
export type { SearchInterface } from './search';
export type { DepositionInterface } from './deposition';

// Auth types needed by AuthProvider and consumers
export type { AuthState, TokenPair, User, SDKConfig } from './types';
export { type TokenStorage, LocalTokenStorage, MemoryTokenStorage } from './storage';

// Mock implementations (for testing)
export { MockSearchNamespace } from './mock/search';
export { MockDepositionNamespace } from './mock/deposition';
