import styles from "./Checkbox.module.css";

export interface CheckboxProps
  extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "type"> {
  /** The clickable label text rendered beside the box. */
  label: React.ReactNode;
}

/**
 * A labelled checkbox — acknowledgment gates ("I understand…"), option
 * toggles. The whole label is the hit target.
 */
export function Checkbox({ label, className, ...rest }: CheckboxProps) {
  return (
    <label className={[styles.wrapper, className].filter(Boolean).join(" ")}>
      <input type="checkbox" className={styles.box} {...rest} />
      <span className={styles.label}>{label}</span>
    </label>
  );
}
