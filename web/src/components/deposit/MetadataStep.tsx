'use client';

import { useState, useRef, useCallback } from 'react';
import styles from './DepositWizard.module.css';
import { formatFileSize, getFileExtension } from './utils';

interface MetadataStepProps {
  conventionName: string;
  file: File | null;
  onFileChange: (file: File) => void;
  onFileRemove: () => void;
}

export function MetadataStep({ conventionName, file, onFileChange, onFileRemove }: MetadataStepProps) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const dropped = e.dataTransfer.files[0];
    if (dropped) onFileChange(dropped);
  }, [onFileChange]);

  return (
    <div className={styles.panel} key="step-2">
      <div className={styles.panelHeader}>
        <div className={styles.panelNumber}>Step 3</div>
        <h2 className={styles.panelTitle}>Upload Metadata</h2>
        <p className={styles.panelDescription}>
          Upload your completed metadata spreadsheet for <strong>{conventionName}</strong>.
        </p>
      </div>
      <div
        className={`${styles.dropzone} ${dragging ? styles.dragging : ''}`}
        onDragOver={e => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".xlsx,.xls,.csv"
          style={{ display: 'none' }}
          onChange={e => {
            const selected = e.target.files?.[0];
            if (selected) onFileChange(selected);
          }}
        />
        <svg className={styles.dropzoneIcon} viewBox="0 0 40 40" fill="none" stroke="currentColor" strokeWidth="1.5">
          <rect x="8" y="4" width="24" height="32" rx="2" />
          <path d="M14 16h12M14 22h12M14 28h8" />
          <path d="M14 4V2M26 4V2" strokeDasharray="2 2" />
        </svg>
        <p className={styles.dropzoneLabel}>
          Drop your spreadsheet here, or <span className={styles.dropzoneLabelAccent}>browse</span>
        </p>
        <p className={styles.dropzoneHint}>.xlsx, .xls, or .csv files accepted</p>
      </div>
      {file && (
        <div className={styles.fileList}>
          <div className={styles.fileItem}>
            <div className={styles.fileIconSpreadsheet}>
              {getFileExtension(file.name).toUpperCase().slice(0, 4)}
            </div>
            <div className={styles.fileInfo}>
              <p className={styles.fileName}>{file.name}</p>
              <span className={styles.fileSize}>{formatFileSize(file.size)}</span>
            </div>
            <div className={styles.fileStatus}>
              <span className={styles.fileStatusDot} />
              Ready
            </div>
            <button className={styles.fileRemove} onClick={onFileRemove} aria-label="Remove file">
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                <path d="M3 3l8 8M11 3l-8 8"/>
              </svg>
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
