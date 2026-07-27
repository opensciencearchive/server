import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { type Services, ServicesProvider } from "@/api/services";

import { makeTestServices } from "./render";

/** Wrapper factory for renderHook with services + a fresh query client. */
export function renderHookWrapper(options?: { services?: Services }) {
  const services = options?.services ?? makeTestServices();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider services={services}>{children}</ServicesProvider>
    </QueryClientProvider>
  );
  return { wrapper, services, queryClient };
}
