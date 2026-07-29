"use client";

import { useEffect, useRef, useState } from "react";

import type { Build, ComponentBuild } from "@/domain/build";

import styles from "./CopyBuildSummary.module.css";

/** Human-readable one-liner for a component's terminal (or in-flight) state. */
function componentLine(component: ComponentBuild): string {
  const { name, kind, status } = component;
  const head = `  - ${name} (${kind}): ${status.kind}`;
  switch (status.kind) {
    case "succeeded":
      return `${head}${status.digest ? ` ${status.digest}` : ` ${status.imageRef}`}`;
    case "failed":
      return `${head} — ${status.errorMessage}`;
    default:
      return head;
  }
}

/** Parent status line, carrying the verbatim error or cancellation cause. */
function statusLine(build: Build): string {
  const { status } = build;
  switch (status.kind) {
    case "build_failed":
    case "publish_failed":
      return `${status.kind}: ${status.errorMessage}`;
    case "cancelled":
      return `cancelled by ${status.cancelledBy}${
        status.cancelReason ? ` — ${status.cancelReason}` : ""
      }`;
    case "published":
      return "published";
    default:
      return status.kind;
  }
}

/** Assemble a paste-ready plain-text summary of a build. */
export function buildSummaryText(build: Build): string {
  const ref = build.conventionRef ? `@${build.conventionRef}` : "";
  return [
    `Build ${build.id}`,
    `Archive ${build.archiveId}`,
    `Convention ${build.conventionSlug}${ref}`,
    `Status ${statusLine(build)}`,
    "Components:",
    ...build.components.map(componentLine),
  ].join("\n");
}

/** Copies a plain-text build summary to the clipboard for pasting into a thread. */
export function CopyBuildSummary({ build }: { build: Build }) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(
    () => () => {
      if (timer.current) clearTimeout(timer.current);
    },
    [],
  );

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(buildSummaryText(build));
      setCopied(true);
      timer.current = setTimeout(() => setCopied(false), 1600);
    } catch {
      // Clipboard unavailable (permissions/insecure context) — do nothing.
    }
  };

  return (
    <button type="button" className={styles.button} onClick={() => void copy()}>
      {copied ? (
        <span className={styles.copied}>Copied</span>
      ) : (
        "Copy build summary"
      )}
    </button>
  );
}
