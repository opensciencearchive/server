import styles from "./PublicFrame.module.css";

/** Centered layout for unauthenticated pages (sign-in, CLI complete). */
export function PublicFrame({ children }: { children: React.ReactNode }) {
  return <div className={styles.frame}>{children}</div>;
}
