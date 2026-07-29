/**
 * Data with NO backing API yet.
 *
 * Produced only by mock service methods (see `api/osa/service.ts` and the
 * mock-marked methods on `AmacrinService`). The brand makes sample data
 * visible in the type system: call sites must unwrap `.data` and can surface
 * a "Sample data" affordance. When a real API ships, delete the `Mocked<…>`
 * wrapper from the method's signature — every affected call site becomes a
 * compile error, so the migration is mechanical and complete.
 */
export type Mocked<T> = { readonly __mock: true; readonly data: T };

export const mocked = <T>(data: T): Mocked<T> => ({ __mock: true, data });
