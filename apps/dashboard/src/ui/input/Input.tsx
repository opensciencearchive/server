import styles from "./Input.module.css";

export interface InputProps
  extends React.InputHTMLAttributes<HTMLInputElement> {
  invalid?: boolean;
  /** Monospace content (slugs, IDs, credentials). */
  mono?: boolean;
}

export function Input({ invalid, mono, className, ...rest }: InputProps) {
  const classes = [
    styles.input,
    mono ? styles.mono : null,
    invalid ? styles.invalid : null,
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return <input className={classes} aria-invalid={invalid || undefined} {...rest} />;
}
