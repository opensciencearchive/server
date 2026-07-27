import type { Tone } from "../tone";

import styles from "./Badge.module.css";

export interface BadgeProps {
  tone?: Tone;
  /** Outline style (role chips) instead of a tinted pill. */
  outline?: boolean;
  withDot?: boolean;
  children: React.ReactNode;
  title?: string;
}

export function Badge({
  tone = "neutral",
  outline = false,
  withDot = false,
  children,
  title,
}: BadgeProps) {
  const classes = [
    styles.badge,
    styles[tone],
    outline ? styles.outline : styles.tinted,
  ].join(" ");
  return (
    <span className={classes} title={title}>
      {withDot && <span className={styles.dot} aria-hidden />}
      {children}
    </span>
  );
}
