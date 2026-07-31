import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { type RenderOptions, render } from "@testing-library/react";

import { MockAmacrinService } from "@/api/amacrin/mock";
import { MockOSAService } from "@/api/osa/mock";
import {
  type PlatformServices,
  type Services,
  ServicesProvider,
} from "@/api/services";

/**
 * In-memory services with a signed-in session — override pieces per test.
 * Returns `PlatformServices` (`amacrin` is always wired), so tests can spy on
 * `services.amacrin` etc. without narrowing. Self-host tests pass an explicit
 * `{ ...svc, isPlatform: false }` as the `services` prop instead.
 */
export function makeTestServices(
  overrides?: Partial<Services>,
): PlatformServices {
  return {
    amacrin: overrides?.amacrin ?? new MockAmacrinService(),
    osa: overrides?.osa ?? new MockOSAService(),
    // Default to a platform-shaped session (amacrin present); self-host tests
    // pass `isPlatform: false`.
    isPlatform: overrides?.isPlatform ?? true,
    // Test services use the in-memory MockOSAService, so tenant data is sample
    // unless a test overrides it.
    tenantDataIsSample: overrides?.tenantDataIsSample ?? true,
  };
}

export function renderWithProviders(
  ui: React.ReactNode,
  options?: RenderOptions & { services?: Services },
) {
  const services = options?.services ?? makeTestServices();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });

  const result = render(
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>{ui}</ServicesProvider>
    </QueryClientProvider>,
    options,
  );
  return { ...result, services, queryClient };
}
