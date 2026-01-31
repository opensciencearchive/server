import type { SearchHit } from '@/types';
import {
  formatOrganism,
  formatSampleCount,
  formatDate,
  getDisplaySrn,
} from '@/lib/utils/format';
import styles from './RecordPreview.module.css';

interface RecordPreviewProps {
  hit: SearchHit;
}

export function RecordPreview({ hit }: RecordPreviewProps) {
  const { srn, score, metadata } = hit;

  return (
    <div className={styles.preview}>
      <header className={styles.header}>
        <div className={styles.headerTop}>
          <code className={styles.srn}>{getDisplaySrn(srn)}</code>
          <span className={styles.score}>{(score * 100).toFixed(0)}% match</span>
        </div>
        <h2 className={styles.title}>{metadata.title}</h2>
        {metadata.gds_type && (
          <span className={styles.type}>{metadata.gds_type}</span>
        )}
      </header>

      {metadata.summary && (
        <section className={styles.section}>
          <h3 className={styles.sectionLabel}>Summary</h3>
          <p className={styles.summary}>{metadata.summary}</p>
        </section>
      )}

      <section className={styles.section}>
        <h3 className={styles.sectionLabel}>Metadata</h3>
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
            <dt className={styles.metaLabel}>Published</dt>
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
        <h3 className={styles.sectionLabel}>Identifier</h3>
        <div className={styles.identifierBox}>
          <code className={styles.fullSrn}>{srn}</code>
          <button
            className={styles.copyButton}
            onClick={() => navigator.clipboard.writeText(srn)}
            aria-label="Copy SRN"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
            </svg>
          </button>
        </div>
      </section>
    </div>
  );
}
