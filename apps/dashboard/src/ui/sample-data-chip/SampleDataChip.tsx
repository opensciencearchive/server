import styles from "./SampleDataChip.module.css";

/**
 * Marks a section rendered from `Mocked<T>` data — no backing API yet.
 * Every unwrap of a Mocked value should surface one of these.
 */
export function SampleDataChip() {
  return (
    <span
      className={styles.chip}
      title="Sample data — this section has no backing API yet"
    >
      Sample data
    </span>
  );
}
