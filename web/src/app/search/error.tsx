'use client';

import { ErrorMessage } from '@/components/ui/ErrorMessage';
import styles from './error.module.css';

interface ErrorProps {
  error: Error;
  reset: () => void;
}

export default function SearchError({ error, reset }: ErrorProps) {
  return (
    <main className={styles.main}>
      <ErrorMessage
        title="Search failed"
        message={error.message || 'An unexpected error occurred while searching.'}
        onRetry={reset}
      />
    </main>
  );
}
