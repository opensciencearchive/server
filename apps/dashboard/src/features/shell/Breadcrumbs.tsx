import Link from "next/link";

import styles from "./Breadcrumbs.module.css";

export interface Crumb {
  label: string;
  href?: string;
  /** Rendered after the label (role or status badge). */
  badge?: React.ReactNode;
}

export function Breadcrumbs({ crumbs }: { crumbs: Crumb[] }) {
  return (
    <nav className={styles.breadcrumbs} aria-label="Breadcrumb">
      {crumbs.map((crumb, index) => (
        <span key={index} className={styles.crumb}>
          <span className={styles.separator} aria-hidden>
            /
          </span>
          {crumb.href ? (
            <Link className={styles.link} href={crumb.href}>
              {crumb.label}
            </Link>
          ) : (
            <span className={styles.current}>{crumb.label}</span>
          )}
          {crumb.badge}
        </span>
      ))}
    </nav>
  );
}
