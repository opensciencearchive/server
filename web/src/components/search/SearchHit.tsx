import Link from 'next/link';
import type { SearchHit as SearchHitType } from '@/types';
import {
  formatScore,
  getDisplaySrn,
  formatOrganism,
  formatSampleCount,
  formatDate,
  truncate,
} from '@/lib/utils/format';
import styles from './SearchHit.module.css';

interface SearchHitProps {
  hit: SearchHitType;
  index: number;
}

export function SearchHit({ hit, index }: SearchHitProps) {
  const { srn, score, metadata } = hit;

  return (
    <Link
      href={`/record/${encodeURIComponent(srn)}`}
      className={styles.card}
    >
      <div className={styles.header}>
        <code className={styles.srn}>{getDisplaySrn(srn)}</code>
        <span className={styles.score}>{formatScore(score)}</span>
      </div>

      <h3 className={styles.title}>{metadata.title}</h3>

      {metadata.summary && (
        <p className={styles.summary}>{truncate(metadata.summary, 200)}</p>
      )}

      <div className={styles.meta}>
        <span className={styles.metaItem}>
          <span className={styles.metaLabel}>Organism</span>
          {formatOrganism(metadata.organism)}
        </span>
        <span className={styles.metaItem}>
          <span className={styles.metaLabel}>Samples</span>
          {formatSampleCount(metadata.sample_count)}
        </span>
        <span className={styles.metaItem}>
          <span className={styles.metaLabel}>Published</span>
          {formatDate(metadata.pub_date)}
        </span>
      </div>
    </Link>
  );
}
