"use client";

import Image from "next/image";
import { useRouter } from "next/navigation";
import { useState } from "react";

import { Button, Field, Input } from "@/ui";

import styles from "./SignInCard.module.css";

/**
 * Self-host login: a dashboard credential (`DASHBOARD_USERNAME`/`PASSWORD`)
 * posted to the BFF, which mints the archive session cookie (issue #173). On
 * success we navigate to `/`, which middleware collapses onto the archive root.
 */
export function PasswordSignInCard() {
  const router = useRouter();
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function onSubmit(event: React.FormEvent) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      if (!res.ok) {
        setError(
          res.status === 401
            ? "Incorrect username or password."
            : "Sign-in failed. Please try again.",
        );
        setSubmitting(false);
        return;
      }
      router.replace("/");
    } catch {
      setError("Couldn't reach the dashboard. Please try again.");
      setSubmitting(false);
    }
  }

  return (
    <div className={styles.card}>
      <Image src="/osa-logo.svg" alt="" width={40} height={40} priority />
      <p className="eyebrow">Open Science Archive</p>
      <h1 className={styles.title}>Sign in</h1>
      <p className={styles.copy}>
        Sign in with your dashboard credentials to manage this archive.
      </p>

      <form onSubmit={onSubmit} className={styles.form}>
        <Field label="Username">
          {({ id, invalid, describedBy }) => (
            <Input
              id={id}
              name="username"
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              invalid={invalid}
              aria-describedby={describedBy}
            />
          )}
        </Field>
        <Field label="Password" error={error ?? undefined}>
          {({ id, invalid, describedBy }) => (
            <Input
              id={id}
              name="password"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              invalid={invalid}
              aria-describedby={describedBy}
            />
          )}
        </Field>
        <Button
          type="submit"
          variant="primary"
          className={styles.submit}
          disabled={submitting}
        >
          {submitting ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </div>
  );
}
