import { useId } from "react";

import styles from "./Field.module.css";

export interface FieldProps {
  label: string;
  /** Right-aligned hint next to the label (e.g. "3-63 chars · a-z 0-9 -"). */
  hint?: string;
  help?: React.ReactNode;
  error?: string | undefined;
  optional?: boolean;
  /** Render prop receives the ids to wire the control. */
  children: (args: {
    id: string;
    describedBy: string | undefined;
    invalid: boolean;
  }) => React.ReactNode;
}

export function Field({ label, hint, help, error, optional, children }: FieldProps) {
  const id = useId();
  const helpId = `${id}-help`;
  const hasNote = Boolean(error ?? help);

  return (
    <div className={styles.field}>
      <div className={styles.labelRow}>
        <label className={styles.label} htmlFor={id}>
          {label}
          {optional && <span className={styles.optional}> Optional</span>}
        </label>
        {hint && <span className={styles.hint}>{hint}</span>}
      </div>
      {children({
        id,
        describedBy: hasNote ? helpId : undefined,
        invalid: Boolean(error),
      })}
      {error ? (
        <p className={styles.error} id={helpId} role="alert">
          {error}
        </p>
      ) : (
        help && (
          <p className={styles.help} id={helpId}>
            {help}
          </p>
        )
      )}
    </div>
  );
}
