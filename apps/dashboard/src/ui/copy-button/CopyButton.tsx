"use client";

import { useEffect, useRef, useState } from "react";

import styles from "./CopyButton.module.css";

export interface CopyButtonProps {
  value: string;
  label?: string;
  size?: "md" | "sm";
}

export function CopyButton({ value, label = "Copy", size = "md" }: CopyButtonProps) {
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
      await navigator.clipboard.writeText(value);
      setCopied(true);
      timer.current = setTimeout(() => setCopied(false), 1600);
    } catch {
      // Clipboard unavailable (permissions/insecure context) — do nothing.
    }
  };

  return (
    <button
      type="button"
      className={[styles.button, size === "sm" ? styles.sm : ""].join(" ")}
      onClick={() => void copy()}
    >
      {copied ? <span className={styles.copied}>Copied</span> : label}
    </button>
  );
}
