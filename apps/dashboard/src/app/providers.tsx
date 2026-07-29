"use client";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { apiModeFromEnv } from "@/api/config";
import { ApiError } from "@/api/http/errors";
import { ServicesProvider } from "@/api/services";

function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 30_000,
        retry: (failureCount, error) => {
          // Client errors are contract outcomes (404/403/409…), not blips.
          if (error instanceof ApiError && error.status < 500) return false;
          return failureCount < 2;
        },
      },
    },
  });
}

/** In `msw` mode, hold rendering until the worker intercepts the network. */
function MswGate({ children }: { children: React.ReactNode }) {
  const [ready, setReady] = useState(apiModeFromEnv() !== "msw");
  useEffect(() => {
    if (ready) return;
    void import("@/mocks/browser").then(({ ensureMswStarted }) =>
      ensureMswStarted().then(() => setReady(true)),
    );
  }, [ready]);
  if (!ready) return null;
  return children;
}

export function Providers({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(makeQueryClient);
  const router = useRouter();

  return (
    <QueryClientProvider client={queryClient}>
      <ServicesProvider
        onSessionLost={() => {
          queryClient.clear();
          router.replace("/sign-in");
        }}
      >
        <MswGate>{children}</MswGate>
      </ServicesProvider>
    </QueryClientProvider>
  );
}
