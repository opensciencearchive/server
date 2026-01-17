import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import styles from './loading.module.css';

export default function RecordLoading() {
  return (
    <main className={styles.main}>
      <LoadingSpinner size="lg" label="Loading record..." />
    </main>
  );
}
