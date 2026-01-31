'use client';

import type { Record } from '@/types';
import {
  formatOrganism,
  formatSampleCount,
  formatDate,
  getDisplaySrn,
} from '@/lib/utils/format';
import styles from './RecordComparison.module.css';

interface RecordComparisonProps {
  records: [Record, Record] | Record[];
}

interface ComparisonRow {
  label: string;
  values: [string, string];
  match: boolean;
}

export function RecordComparison({ records }: RecordComparisonProps) {
  if (records.length !== 2) {
    return <div className={styles.error}>Comparison requires exactly 2 records</div>;
  }

  const [recordA, recordB] = records;
  const metaA = recordA.metadata;
  const metaB = recordB.metadata;

  // Build comparison rows
  const rows: ComparisonRow[] = [
    {
      label: 'Organism',
      values: [formatOrganism(metaA.organism), formatOrganism(metaB.organism)],
      match: metaA.organism === metaB.organism,
    },
    {
      label: 'Samples',
      values: [formatSampleCount(metaA.sample_count), formatSampleCount(metaB.sample_count)],
      match: metaA.sample_count === metaB.sample_count,
    },
    {
      label: 'Published',
      values: [formatDate(metaA.pub_date), formatDate(metaB.pub_date)],
      match: metaA.pub_date === metaB.pub_date,
    },
    {
      label: 'Platform',
      values: [metaA.platform ?? '—', metaB.platform ?? '—'],
      match: metaA.platform === metaB.platform,
    },
    {
      label: 'Type',
      values: [metaA.gds_type ?? '—', metaB.gds_type ?? '—'],
      match: metaA.gds_type === metaB.gds_type,
    },
    {
      label: 'Entry Type',
      values: [metaA.entry_type ?? '—', metaB.entry_type ?? '—'],
      match: metaA.entry_type === metaB.entry_type,
    },
  ];

  return (
    <div className={styles.container}>
      {/* Title cards */}
      <div className={styles.headerRow}>
        <div className={styles.headerCell}>
          <code className={styles.srn}>{getDisplaySrn(recordA.srn)}</code>
          <div className={styles.title}>{metaA.title}</div>
        </div>
        <div className={styles.headerCell}>
          <code className={styles.srn}>{getDisplaySrn(recordB.srn)}</code>
          <div className={styles.title}>{metaB.title}</div>
        </div>
      </div>

      {/* Comparison rows */}
      <div className={styles.table}>
        {rows.map((row) => (
          <div key={row.label} className={styles.row}>
            <div className={styles.labelCell}>{row.label}</div>
            <div className={styles.valueCell}>{row.values[0]}</div>
            <div className={styles.valueCell}>{row.values[1]}</div>
            <div className={styles.matchCell}>
              {row.match && row.values[0] !== '—' && (
                <span className={styles.matchIndicator}>✓</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Summary comparison */}
      <div className={styles.summarySection}>
        <div className={styles.summaryHeader}>Summary</div>
        <div className={styles.summaryRow}>
          <div className={styles.summaryCell}>
            {metaA.summary || <span className={styles.empty}>No summary available</span>}
          </div>
          <div className={styles.summaryCell}>
            {metaB.summary || <span className={styles.empty}>No summary available</span>}
          </div>
        </div>
      </div>
    </div>
  );
}
