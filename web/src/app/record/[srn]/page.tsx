import { notFound } from 'next/navigation';
import Link from 'next/link';
import { osa } from '@/lib/sdk';
import { RecordDetail } from '@/components/record/RecordDetail';
import { BackButton } from '@/components/ui/BackButton';
import { isApiError } from '@/types';
import styles from './page.module.css';

interface RecordPageProps {
  params: Promise<{ srn: string }>;
}

export default async function RecordPage({ params }: RecordPageProps) {
  const { srn } = await params;
  const decodedSrn = decodeURIComponent(srn);

  let record;
  try {
    const result = await osa.search.getRecord(decodedSrn);
    record = result.record;
  } catch (error) {
    if (isApiError(error) && error.detail.includes('not found')) {
      notFound();
    }
    throw error;
  }

  return (
    <main className={styles.main}>
      <div className={styles.container}>
        <nav className={styles.breadcrumb}>
          <Link href="/" className={styles.breadcrumbLink}>
            Home
          </Link>
          <span className={styles.separator}>/</span>
          <span className={styles.current}>Record</span>
        </nav>

        <RecordDetail record={record} />

        <div className={styles.backLink}>
          <BackButton fallbackHref="/search">
            ← Back to search
          </BackButton>
        </div>
      </div>
    </main>
  );
}
