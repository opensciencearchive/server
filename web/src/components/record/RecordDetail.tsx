'use client';

import type { Record } from '@/types';
import {
  formatOrganism,
  formatSampleCount,
  formatDate,
  getDisplaySrn,
} from '@/lib/utils/format';
import styles from './RecordDetail.module.css';

interface RecordDetailProps {
  record: Record;
}

interface MetadataRow {
  label: string;
  value: string;
}

export function RecordDetail({ record }: RecordDetailProps) {
  const { srn, metadata } = record;

  // Build metadata rows in same order as comparison
  const rows: MetadataRow[] = [
    { label: 'Organism', value: formatOrganism(metadata.organism) },
    { label: 'Samples', value: formatSampleCount(metadata.sample_count) },
    { label: 'Published', value: formatDate(metadata.pub_date) },
    { label: 'Platform', value: metadata.platform ?? '—' },
    { label: 'Type', value: metadata.gds_type ?? '—' },
    { label: 'Entry Type', value: metadata.entry_type ?? '—' },
  ];

  return (
    <div className={styles.container}>
      {/* Header - matches comparison headerCell */}
      <div className={styles.header}>
        <code className={styles.srn}>{getDisplaySrn(srn)}</code>
        <div className={styles.title}>{metadata.title}</div>
      </div>

      {/* Metadata rows - matches comparison table */}
      <div className={styles.table}>
        {rows.map((row) => (
          <div key={row.label} className={styles.row}>
            <div className={styles.labelCell}>{row.label}</div>
            <div className={styles.valueCell}>{row.value}</div>
          </div>
        ))}
      </div>

      {/* Summary - matches comparison summarySection */}
      <div className={styles.summarySection}>
        <div className={styles.summaryHeader}>Summary</div>
        <div className={styles.summaryCell}>
          {metadata.summary || <span className={styles.empty}>No summary available</span>}
        </div>
      </div>
    </div>
  );
}
