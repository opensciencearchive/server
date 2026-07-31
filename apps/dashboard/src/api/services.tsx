"use client";

/**
 * Dependency injection for the data layer.
 *
 * `isPlatformFromEnv()` selects the runtime identity (issue #173):
 *
 * - **self-hosted** (`IS_PLATFORM=false`, default) — no cloud control plane.
 *   Only `osa` is wired; `amacrin` is absent. Access is a dashboard-credential
 *   session handled by the BFF.
 * - **platform** (`IS_PLATFORM=true`) — the cloud wiring. The browser holds no
 *   token; `RealAmacrinService` calls the same-origin BFF proxy (`/api/amacrin`),
 *   which attaches the sealed bearer server-side (#185). `NEXT_PUBLIC_API_MODE`
 *   picks the backing:
 *     real — RealAmacrinService through the BFF proxy (default)
 *     mock — MockAmacrinService, fully in-memory (demos, previews)
 *     msw  — real service, with the Mock Service Worker intercepting
 *
 * Platform-only consumers must read services through `usePlatformServices()`,
 * which narrows the optional `amacrin` field (and throws in a self-host build —
 * those components are never rendered there).
 */
import { createContext, useContext, useMemo } from "react";

import {
  type ApiMode,
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

/** Same-origin base for control-plane calls through the platform BFF proxy. */
const AMACRIN_PROXY_BASE = "/api/amacrin";

export interface Services {
  osa: OSAService;
  /** Which runtime identity built these services. */
  isPlatform: boolean;
  /** Cloud control-plane service — present only in a platform build. */
  amacrin?: AmacrinService;
}

/** `Services` with the platform-only field narrowed to non-optional. */
export interface PlatformServices extends Services {
  amacrin: AmacrinService;
}

export function buildServices(opts: {
  mode: ApiMode;
  isPlatform: boolean;
  onSessionLost?: () => void;
}): Services {
  if (!opts.isPlatform) {
    // Self-hosted: no cloud control plane. Project-level data comes from the
    // local archive via the same-origin BFF proxy.
    return { osa: new RealOSAService(apiProxyBaseUrl()), isPlatform: false };
  }

  const osa = new MockOSAService();

  if (opts.mode === "mock") {
    return { amacrin: new MockAmacrinService(), osa, isPlatform: true };
  }

  // real + msw: the browser holds no token. Control-plane calls go same-origin
  // through the BFF proxy (`/api/amacrin`), which attaches the sealed bearer and
  // refreshes it server-side; a 401 means the session is gone → sign out.
  const client = new HttpClient({
    baseUrl: AMACRIN_PROXY_BASE,
    onUnauthorized: opts.onSessionLost,
  });
  const amacrin = new RealAmacrinService({ baseUrl: AMACRIN_PROXY_BASE, client });
  return { amacrin, osa, isPlatform: true };
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
  if (services.amacrin === undefined) {
    throw new Error(
      "Platform services are unavailable in a self-hosted (IS_PLATFORM=false) build.",
    );
  }
  return { ...services, amacrin: services.amacrin };
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
