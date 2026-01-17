'use client';

import type { Record } from '@/types';
import {
  formatOrganism,
  formatSampleCount,
  formatDate,
} from '@/lib/utils/format';
import styles from './RecordDetail.module.css';

interface RecordDetailProps {
  record: Record;
}

export function RecordDetail({ record }: RecordDetailProps) {
  const { srn, metadata } = record;

  return (
    <article className={styles.article}>
      <header className={styles.header}>
        <code className={styles.srn}>{srn}</code>
        <h1 className={styles.title}>{metadata.title}</h1>
        {metadata.gds_type && (
          <span className={styles.type}>{metadata.gds_type}</span>
        )}
      </header>

      {metadata.summary && (
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>Summary</h2>
          <p className={styles.summary}>{metadata.summary}</p>
        </section>
      )}

      <section className={styles.section}>
        <h2 className={styles.sectionTitle}>Details</h2>
        <dl className={styles.metaGrid}>
          <div className={styles.metaItem}>
            <dt className={styles.metaLabel}>Organism</dt>
            <dd className={styles.metaValue}>{formatOrganism(metadata.organism)}</dd>
          </div>

          <div className={styles.metaItem}>
            <dt className={styles.metaLabel}>Samples</dt>
            <dd className={styles.metaValue}>{formatSampleCount(metadata.sample_count)}</dd>
          </div>

          <div className={styles.metaItem}>
            <dt className={styles.metaLabel}>Publication Date</dt>
            <dd className={styles.metaValue}>{formatDate(metadata.pub_date)}</dd>
          </div>

          {metadata.platform && (
            <div className={styles.metaItem}>
              <dt className={styles.metaLabel}>Platform</dt>
              <dd className={styles.metaValue}>{metadata.platform}</dd>
            </div>
          )}

          {metadata.entry_type && (
            <div className={styles.metaItem}>
              <dt className={styles.metaLabel}>Entry Type</dt>
              <dd className={styles.metaValue}>{metadata.entry_type}</dd>
            </div>
          )}
        </dl>
      </section>

      <section className={styles.section}>
        <h2 className={styles.sectionTitle}>Identifier</h2>
        <div className={styles.identifierBox}>
          <code className={styles.fullSrn}>{srn}</code>
          <button
            className={styles.copyButton}
            onClick={() => navigator.clipboard.writeText(srn)}
            aria-label="Copy SRN to clipboard"
          >
            Copy
          </button>
        </div>
      </section>
    </article>
  );
}
