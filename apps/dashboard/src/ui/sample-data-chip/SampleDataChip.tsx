import styles from "./SampleDataChip.module.css";

/**
 * Marks a section rendered from sample values rather than the archive's own
 * data — the tenant read path (`MockOSAService`, `tenantDataIsSample`) on a
 * platform build. Control-plane surfaces are all real and never show this.
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
