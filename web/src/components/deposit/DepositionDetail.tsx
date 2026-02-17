'use client';

import Link from 'next/link';
import type { Deposition, DepositionStatus } from '@/types/deposition';
import styles from './DepositionDetail.module.css';

interface DepositionDetailProps {
  deposition: Deposition;
}

const STATUS_CLASS: Record<DepositionStatus, string> = {
  draft: styles.statusDraft,
  in_validation: styles.statusInValidation,
  in_review: styles.statusInReview,
  accepted: styles.statusAccepted,
  rejected: styles.statusRejected,
};

const STATUS_LABEL: Record<DepositionStatus, string> = {
  draft: 'Draft',
  in_validation: 'In Validation',
  in_review: 'In Review',
  accepted: 'Accepted',
  rejected: 'Rejected',
};

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function getFileExtension(name: string): string {
  const dot = name.lastIndexOf('.');
  return dot >= 0 ? name.slice(dot + 1).toUpperCase() : 'FILE';
}

export function DepositionDetail({ deposition }: DepositionDetailProps) {
  const { srn, convention_srn, status, metadata, files, record_srn, created_at, updated_at } = deposition;

  const metadataEntries = Object.entries(metadata);

  return (
    <div className={styles.container}>
      {/* Header */}
      <div className={styles.header}>
        <div className={styles.headerTop}>
          <code className={styles.srn}>{srn}</code>
          <span className={STATUS_CLASS[status]}>{STATUS_LABEL[status]}</span>
        </div>
        <div className={styles.conventionSrn}>Convention: {convention_srn}</div>
      </div>

      {/* Timestamps & info */}
      <div className={styles.table}>
        <div className={styles.row}>
          <div className={styles.labelCell}>Created</div>
          <div className={styles.valueCell}>{formatDate(created_at)}</div>
        </div>
        <div className={styles.row}>
          <div className={styles.labelCell}>Updated</div>
          <div className={styles.valueCell}>{formatDate(updated_at)}</div>
        </div>
        {record_srn && (
          <div className={styles.row}>
            <div className={styles.labelCell}>Record</div>
            <div className={styles.valueCell}>
              <Link
                href={`/record/${encodeURIComponent(record_srn)}`}
                className={styles.recordLink}
              >
                {record_srn}
              </Link>
            </div>
          </div>
        )}
      </div>

      {/* Metadata */}
      {metadataEntries.length > 0 && (
        <div className={styles.section}>
          <div className={styles.sectionHeader}>Metadata</div>
          <div className={styles.metadataTable}>
            {metadataEntries.map(([key, value]) => (
              <div key={key} className={styles.metadataRow}>
                <div className={styles.metadataKey}>{key}</div>
                <div className={styles.metadataValue}>{String(value)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Files */}
      <div className={styles.section}>
        <div className={styles.sectionHeader}>Files ({files.length})</div>
        {files.length > 0 ? (
          <div className={styles.fileList}>
            {files.map((file) => (
              <div key={file.name} className={styles.fileItem}>
                <div className={styles.fileIcon}>{getFileExtension(file.name)}</div>
                <div className={styles.fileInfo}>
                  <div className={styles.fileName}>{file.name}</div>
                  <span className={styles.fileMeta}>
                    {formatFileSize(file.size)} &middot; {formatDate(file.uploaded_at)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className={styles.empty}>No files uploaded</div>
        )}
      </div>
    </div>
  );
}
