"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { requirePlatform, useServices } from "@/api/services";

import styles from "./AuthGuard.module.css";

type GuardState = "checking" | "authed" | "anonymous";

/**
 * Client-side route protection.
 *
 * Platform: the refresh cookie belongs to the API origin (Path=/api/v1/auth),
 * so neither Next middleware nor server components can see it — the only way to
 * know whether a session exists is the bootstrap POST /auth/refresh here.
 *
 * Self-host: the session is a same-origin httpOnly cookie that `middleware.ts`
 * guards (M2b), so there is nothing to bootstrap client-side — render straight
 * through.
 */
export function AuthGuard({ children }: { children: React.ReactNode }) {
  const services = useServices();
  const router = useRouter();
  const [state, setState] = useState<GuardState>(
    services.isPlatform ? "checking" : "authed",
  );

  useEffect(() => {
    if (!services.isPlatform) return undefined;
    let cancelled = false;
    requirePlatform(services)
      .refresher.ensureFreshToken()
      .then((token) => {
        if (!cancelled) setState(token !== null ? "authed" : "anonymous");
      })
      .catch(() => {
        // Transport failure — treat as signed out rather than hanging.
        if (!cancelled) setState("anonymous");
      });
    return () => {
      cancelled = true;
    };
  }, [services]);

  useEffect(() => {
    if (state === "anonymous") router.replace("/sign-in");
  }, [state, router]);

  if (state !== "authed") {
    return (
      <div className={styles.boot} aria-busy="true">
        <div className={styles.mark} />
        <p className={styles.note}>Restoring your session…</p>
      </div>
    );
  }
  return children;
}
