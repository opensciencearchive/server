'use client';

import { useState, useEffect } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { osa } from '@/lib/sdk';
import { DepositionDetail } from '@/components/deposit/DepositionDetail';
import { BackButton } from '@/components/ui/BackButton';
import type { Deposition } from '@/types';
import styles from './page.module.css';

export default function DepositionPage() {
  const params = useParams<{ srn: string }>();
  const decodedSrn = decodeURIComponent(params.srn);

  const [deposition, setDeposition] = useState<Deposition | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    osa.deposition.get(decodedSrn).then(setDeposition).catch((err) => {
      setError(err?.message ?? 'Failed to load deposition');
    });
  }, [decodedSrn]);

  return (
    <main className={styles.main}>
      <div className={styles.container}>
        <nav className={styles.breadcrumb}>
          <Link href="/" className={styles.breadcrumbLink}>
            Home
          </Link>
          <span className={styles.separator}>/</span>
          <span className={styles.current}>Deposition</span>
        </nav>

        {error && <p className={styles.error}>{error}</p>}
        {!deposition && !error && <p className={styles.loading}>Loading...</p>}
        {deposition && <DepositionDetail deposition={deposition} />}

        <div className={styles.backLink}>
          <BackButton fallbackHref="/deposit">
            ← New deposition
          </BackButton>
        </div>
      </div>
    </main>
  );
}
