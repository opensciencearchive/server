import type { Tone } from "../tone";

import styles from "./StatusDot.module.css";

export interface StatusDotProps {
  tone: Tone;
  label: string;
  /** Animate for in-flight states (deploying, building…). */
  pulse?: boolean;
  /** Monospace count-style rendering, e.g. "1 running". */
  mono?: boolean;
}

export function StatusDot({ tone, label, pulse, mono = true }: StatusDotProps) {
  return (
    <span className={[styles.wrap, mono ? styles.mono : ""].join(" ")}>
      <span
        className={[styles.dot, styles[tone], pulse ? styles.pulse : ""].join(" ")}
        aria-hidden
      />
      <span className={styles[`text-${tone}`]}>{label}</span>
    </span>
  );
}
