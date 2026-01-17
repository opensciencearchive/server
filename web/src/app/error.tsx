'use client';

import { ErrorMessage } from '@/components/ui/ErrorMessage';
import styles from './error.module.css';

interface ErrorProps {
  error: Error;
  reset: () => void;
}

export default function GlobalError({ error, reset }: ErrorProps) {
  return (
    <main className={styles.main}>
      <ErrorMessage
        title="Something went wrong"
        message={error.message || 'An unexpected error occurred. Please try again.'}
        onRetry={reset}
      />
    </main>
  );
}
