"use client";

/**
 * Dependency injection for the data layer.
 *
 * `isPlatformFromEnv()` selects the runtime identity (issue #173):
 *
 * - **self-hosted** (`IS_PLATFORM=false`, default) — no cloud control plane.
 *   Only `osa` is wired; `amacrin`/`refresher`/`tokenStore` are absent. Access
 *   is a dashboard-credential session handled by the BFF, not the Amacrin
 *   bearer machinery.
 * - **platform** (`IS_PLATFORM=true`) — the existing cloud wiring.
 *   `NEXT_PUBLIC_API_MODE` picks the Amacrin backing:
 *     real — RealAmacrinService against NEXT_PUBLIC_API_URL (default)
 *     mock — MockAmacrinService, fully in-memory (demos, previews)
 *     msw  — real service, with the Mock Service Worker intercepting
 *
 * Platform-only consumers must read services through `usePlatformServices()`,
 * which narrows the optional fields (and throws in a self-host build — those
 * components are never rendered there).
 */
import { createContext, useContext, useMemo } from "react";

import { SessionRefresher } from "./auth/refresher";
import { TokenStore } from "./auth/token-store";
import {
  type ApiMode,
  apiBaseUrlFromEnv,
  apiModeFromEnv,
  apiProxyBaseUrl,
  isPlatformFromEnv,
} from "./config";
import { HttpClient } from "./http/client";
import { MockAmacrinService } from "./amacrin/mock";
import { RealAmacrinService } from "./amacrin/real";
import type { AmacrinService } from "./amacrin/service";
import { MockOSAService } from "./osa/mock";
import { RealOSAService } from "./osa/real";
import type { OSAService } from "./osa/service";

export type { ApiMode };

export interface Services {
  osa: OSAService;
  /** Which runtime identity built these services. */
  isPlatform: boolean;
  /** Cloud control-plane services — present only in a platform build. */
  amacrin?: AmacrinService;
  refresher?: SessionRefresher;
  tokenStore?: TokenStore;
}

/** `Services` with the platform-only fields narrowed to non-optional. */
export interface PlatformServices extends Services {
  amacrin: AmacrinService;
  refresher: SessionRefresher;
  tokenStore: TokenStore;
}

export function buildServices(opts: {
  mode: ApiMode;
  baseUrl: string;
  isPlatform: boolean;
  onSessionLost?: () => void;
}): Services {
  if (!opts.isPlatform) {
    // Self-hosted: no cloud control plane. Project-level data comes from the
    // local archive via the same-origin BFF proxy; there is no Amacrin session
    // machinery to construct (constructing the refresher would fire dead
    // /auth/refresh calls at a nonexistent cloud API).
    return { osa: new RealOSAService(apiProxyBaseUrl()), isPlatform: false };
  }

  const osa = new MockOSAService();
  const tokenStore = new TokenStore();

  if (opts.mode === "mock") {
    const amacrin = new MockAmacrinService();
    const refresher = new SessionRefresher({
      store: tokenStore,
      refreshFn: () => amacrin.refreshSession(),
      onSessionLost: opts.onSessionLost,
    });
    return { amacrin, osa, refresher, tokenStore, isPlatform: true };
  }

  // real + msw: the refresher calls the service's cookie-authenticated
  // refresh, which never goes through the bearer-token client — the lazy
  // ref breaks the construction cycle.
  const serviceRef: { current: RealAmacrinService | null } = { current: null };
  const refresher = new SessionRefresher({
    store: tokenStore,
    refreshFn: () => serviceRef.current!.refreshSession(),
    onSessionLost: opts.onSessionLost,
  });
  const client = new HttpClient({ baseUrl: opts.baseUrl, refresher });
  const amacrin = new RealAmacrinService({ baseUrl: opts.baseUrl, client });
  serviceRef.current = amacrin;
  return { amacrin, osa, refresher, tokenStore, isPlatform: true };
}

const ServicesContext = createContext<Services | null>(null);

export function ServicesProvider({
  services,
  onSessionLost,
  children,
}: {
  /** Tests inject fakes here; the app builds from env. */
  services?: Services;
  onSessionLost?: () => void;
  children: React.ReactNode;
}) {
  const value = useMemo(
    () =>
      services ??
      buildServices({
        mode: apiModeFromEnv(),
        baseUrl: apiBaseUrlFromEnv(),
        isPlatform: isPlatformFromEnv(),
        onSessionLost,
      }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- services are built once per mount
    [services],
  );
  return (
    <ServicesContext.Provider value={value}>
      {children}
    </ServicesContext.Provider>
  );
}

export function useServices(): Services {
  const services = useContext(ServicesContext);
  if (services === null) {
    throw new Error("useServices must be used inside <ServicesProvider>");
  }
  return services;
}

/**
 * Narrow `Services` to `PlatformServices`, or throw if the cloud fields are
 * absent (a self-host build). Use from shared code that branches on
 * `services.isPlatform` and needs the cloud services only on the platform arm.
 */
export function requirePlatform(services: Services): PlatformServices {
  if (
    services.amacrin === undefined ||
    services.refresher === undefined ||
    services.tokenStore === undefined
  ) {
    throw new Error(
      "Platform services are unavailable in a self-hosted (IS_PLATFORM=false) build.",
    );
  }
  return {
    ...services,
    amacrin: services.amacrin,
    refresher: services.refresher,
    tokenStore: services.tokenStore,
  };
}

/**
 * Read the cloud control-plane services. Use this from platform-only components
 * (orgs, archive lifecycle, builds, deployments, cloud auth). Throws in a
 * self-host build — such components are never mounted there (they sit behind
 * `IS_PLATFORM=true` in routing and nav), so the throw is a guardrail, not a
 * runtime path.
 */
export function usePlatformServices(): PlatformServices {
  return requirePlatform(useServices());
}
