"use client";

import Image from "next/image";

import { signInUrl } from "./sign-in-url";
import styles from "./SignInCard.module.css";

const ERROR_COPY: Record<string, string> = {
  provider_error:
    "Google didn't return a valid response. Nothing was created — try again.",
  expired: "The sign-in attempt expired. Start again from the beginning.",
  invalid: "The sign-in attempt was not valid. Start again from the beginning.",
};

export function SignInCard({
  apiBaseUrl,
  error,
}: {
  apiBaseUrl: string;
  error: string | null;
}) {
  // The redirect target is this deployment's own origin — computed in the
  // browser so the same build works on any host (localhost, preview, prod).
  const origin =
    typeof window === "undefined" ? "" : window.location.origin;
  const href = signInUrl(apiBaseUrl, origin);

  return (
    <div className={styles.card}>
      <Image src="/osa-logo.svg" alt="" width={40} height={40} priority />
      <p className="eyebrow">Amacrin Cloud</p>

      {error === null ? (
        <>
          <h1 className={styles.title}>Deploy an archive</h1>
          <p className={styles.copy}>
            Hosted OSA archives for research groups — sign in to create and
            manage yours.
          </p>
          <a className={styles.google} href={href}>
            Continue with Google
          </a>
          <p className={styles.note}>
            First sign-in creates your account and a Personal organisation.
            Signing in again just picks up where you left off.
          </p>
        </>
      ) : (
        <>
          <div className={styles.errorIcon} aria-hidden>
            !
          </div>
          <h1 className={styles.title}>Sign-in didn&apos;t complete</h1>
          <p className={styles.copy}>
            {ERROR_COPY[error] ?? ERROR_COPY["invalid"]}
          </p>
          <a className={styles.retry} href={href}>
            Try again
          </a>
        </>
      )}

      <footer className={styles.footer}>
        <span>Documentation</span>
        <span>·</span>
        <span>Status</span>
        <span>·</span>
        <span className="mono">v1.0.0</span>
      </footer>
    </div>
  );
}
