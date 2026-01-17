import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import styles from './loading.module.css';

export default function SearchLoading() {
  return (
    <main className={styles.main}>
      <LoadingSpinner size="lg" label="Searching records..." />
    </main>
  );
}
