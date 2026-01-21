import Image from 'next/image';
import styles from './Footer.module.css';

export function Footer() {
  return (
    <footer className={styles.footer}>
      <div className={styles.inner}>
        <div className={styles.primary}>
          <a
            href="https://opensciencearchive.org"
            target="_blank"
            rel="noopener noreferrer"
            className={styles.brand}
          >
            <Image
              src="/osa_logo.svg"
              alt="Open Science Archive"
              width={24}
              height={24}
              className={styles.logo}
            />
            <span className={styles.brandName}>Open Science Archive</span>
          </a>
          <p className={styles.mission}>
            Open-source, domain-agnostic scientific data infrastructure
          </p>
        </div>

        <nav className={styles.nav}>
          <div className={styles.navGroup}>
            <h4 className={styles.navTitle}>Resources</h4>
            <a
              href="https://opensciencearchive.org"
              className={styles.navLink}
              target="_blank"
              rel="noopener noreferrer"
            >
              OSA Home
            </a>
            <a
              href="https://spec.opensciencearchive.org"
              className={styles.navLink}
              target="_blank"
              rel="noopener noreferrer"
            >
              Specification
            </a>
            <a
              href="https://docs.opensciencearchive.org"
              className={styles.navLink}
              target="_blank"
              rel="noopener noreferrer"
            >
              Documentation
            </a>
          </div>
          <div className={styles.navGroup}>
            <h4 className={styles.navTitle}>Community</h4>
            <a
              href="https://github.com/opensciencearchive"
              className={styles.navLink}
              target="_blank"
              rel="noopener noreferrer"
            >
              GitHub
            </a>
            <a
              href="https://opensciencearchive.zulipchat.com/join/uuetdamcz55a2otndpkytkq5/"
              className={styles.navLink}
              target="_blank"
              rel="noopener noreferrer"
            >
              Zulip Chat
            </a>
          </div>
        </nav>
      </div>

      <div className={styles.bottom}>
        <p className={styles.copyright}>
          Apache 2.0 License
        </p>
        <span className={styles.version}>v0.1.0</span>
      </div>
    </footer>
  );
}
