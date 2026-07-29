import styles from "./Select.module.css";

export function Select({
  className,
  ...rest
}: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <span className={styles.wrapper}>
      <select
        className={[styles.select, className].filter(Boolean).join(" ")}
        {...rest}
      />
      <span className={styles.chevron} aria-hidden>
        ▾
      </span>
    </span>
  );
}
