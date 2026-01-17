'use client';

import { ErrorMessage } from '@/components/ui/ErrorMessage';
import styles from './error.module.css';

interface ErrorProps {
  error: Error;
  reset: () => void;
}

export default function RecordError({ error, reset }: ErrorProps) {
  return (
    <main className={styles.main}>
      <ErrorMessage
        title="Failed to load record"
        message={error.message || 'An unexpected error occurred while loading this record.'}
        onRetry={reset}
      />
    </main>
  );
}
