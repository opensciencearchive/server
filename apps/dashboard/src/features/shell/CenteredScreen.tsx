import styles from "./CenteredScreen.module.css";

/**
 * Full-page, chrome-less centered layout for standalone authed screens — the
 * post-create deploy-progress page and the archive-not-found notice. Mirrors the
 * unauthenticated `PublicFrame`, but without the public-layout semantics.
 */
export function CenteredScreen({ children }: { children: React.ReactNode }) {
  return <div className={styles.screen}>{children}</div>;
}
