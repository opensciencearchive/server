"use client";

import Image from "next/image";
import Link from "next/link";

import { DOCS_URL } from "./links";
import { UserMenu } from "./UserMenu";
import styles from "./AppShell.module.css";

export interface AppShellProps {
  /** Breadcrumb content after the logo. */
  breadcrumbs?: React.ReactNode;
  /** Right side of the header before the avatar (e.g. Visit archive). */
  headerActions?: React.ReactNode;
  sidebar?: React.ReactNode;
  children: React.ReactNode;
}

export function AppShell({
  breadcrumbs,
  headerActions,
  sidebar,
  children,
}: AppShellProps) {
  return (
    <div className={styles.shell}>
      <header className={styles.header}>
        <div className={styles.headerLeft}>
          <Link href="/" className={styles.logo} aria-label="Amacrin home">
            <Image src="/osa-logo.svg" alt="" width={24} height={24} />
          </Link>
          {breadcrumbs}
        </div>
        <div className={styles.headerRight}>
          {headerActions}
          <a
            className={styles.docsLink}
            href={DOCS_URL}
            target="_blank"
            rel="noreferrer"
          >
            Docs
          </a>
          <UserMenu />
        </div>
      </header>
      <div className={styles.body}>
        {sidebar && <aside className={styles.sidebar}>{sidebar}</aside>}
        <main className={styles.main}>{children}</main>
      </div>
    </div>
  );
}
