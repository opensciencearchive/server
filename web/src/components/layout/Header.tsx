import Image from 'next/image';
import Link from 'next/link';
import styles from './Header.module.css';

export function Header() {
  return (
    <header className={styles.header}>
      <div className={styles.inner}>
        <Link href="/" className={styles.logo}>
          <Image
            src="/osa_logo.svg"
            alt="Open Science Archive"
            width={28}
            height={28}
            className={styles.logoImage}
          />
          <span className={styles.logoText}>Lingual Bio</span>
        </Link>
      </div>
    </header>
  );
}
