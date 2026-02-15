'use client';

import Image from 'next/image';
import Link from 'next/link';
import styles from './Header.module.css';
import { AuthButtons } from './AuthButtons';
import { useAuth } from '@/hooks/useAuth';

export function Header() {
  const { isAuthenticated, isLoading } = useAuth();

  return (
    <header className={styles.header}>
      <div className={styles.inner}>
        <div className={styles.left}>
          <Link href="/" className={styles.logo}>
            <Image
              src="/osa_logo.svg"
              alt="Open Science Archive"
              width={28}
              height={28}
              className={styles.logoImage}
            />
            <span className={styles.logoText}>OSA</span>
          </Link>
          <nav className={styles.nav}>
            <Link href="/search" className={styles.navLink}>
              <svg className={styles.navLinkIcon} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="7" cy="7" r="4.5" />
                <path d="M10.5 10.5L14 14" />
              </svg>
              Search
            </Link>
            {!isLoading && isAuthenticated && (
              <Link href="/deposit" className={styles.depositLink}>
                <svg className={styles.navLinkIcon} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M8 2v8M5 7l3 3 3-3" />
                  <path d="M2 12v1.5a.5.5 0 00.5.5h11a.5.5 0 00.5-.5V12" />
                </svg>
                Deposit
              </Link>
            )}
          </nav>
        </div>
        <AuthButtons />
      </div>
    </header>
  );
}
