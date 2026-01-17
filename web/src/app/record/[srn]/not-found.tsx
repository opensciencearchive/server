import { BackButton } from '@/components/ui/BackButton';
import styles from './not-found.module.css';

export default function RecordNotFound() {
  return (
    <main className={styles.main}>
      <div className={styles.container}>
        <div className={styles.icon}>
          <svg
            width="32"
            height="32"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
            <line x1="8" y1="8" x2="14" y2="14" />
            <line x1="14" y1="8" x2="8" y2="14" />
          </svg>
        </div>
        <h1 className={styles.title}>Record not found</h1>
        <p className={styles.message}>
          The record you're looking for doesn't exist or may have been removed.
        </p>
        <div className={styles.link}>
          <BackButton fallbackHref="/search">
            ← Back to search
          </BackButton>
        </div>
      </div>
    </main>
  );
}
