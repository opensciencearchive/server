'use client';

import { useState, useRef, useCallback } from 'react';
import styles from './DepositWizard.module.css';
import { formatFileSize, getFileExtension, getFileIconType } from './utils';

interface DataFilesStepProps {
  files: File[];
  onFilesAdd: (files: File[]) => void;
  onFileRemove: (index: number) => void;
}

export function DataFilesStep({ files, onFilesAdd, onFileRemove }: DataFilesStepProps) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const dropped = Array.from(e.dataTransfer.files);
    if (dropped.length > 0) onFilesAdd(dropped);
  }, [onFilesAdd]);

  return (
    <div className={styles.panel} key="step-3">
      <div className={styles.panelHeader}>
        <div className={styles.panelNumber}>Step 4</div>
        <h2 className={styles.panelTitle}>Upload Data Files</h2>
        <p className={styles.panelDescription}>
          Upload the data files associated with your deposition. You can add multiple files at once.
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
          multiple
          style={{ display: 'none' }}
          onChange={e => {
            const newFiles = Array.from(e.target.files ?? []);
            if (newFiles.length > 0) onFilesAdd(newFiles);
          }}
        />
        <svg className={styles.dropzoneIcon} viewBox="0 0 40 40" fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M20 12v16M12 20h16" />
          <rect x="4" y="4" width="32" height="32" rx="4" />
        </svg>
        <p className={styles.dropzoneLabel}>
          Drop files here, or <span className={styles.dropzoneLabelAccent}>browse</span>
        </p>
        <p className={styles.dropzoneHint}>
          FASTQ, BAM, CSV, HDF5, Parquet, or any data format
        </p>
      </div>
      {files.length > 0 && (
        <div className={styles.fileList}>
          {files.map((file, idx) => {
            const iconType = getFileIconType(file.name);
            const iconClass =
              iconType === 'spreadsheet' ? styles.fileIconSpreadsheet :
              iconType === 'data' ? styles.fileIconData :
              styles.fileIconGeneric;
            return (
              <div key={`${file.name}-${idx}`} className={styles.fileItem} style={{ animationDelay: `${idx * 50}ms` }}>
                <div className={iconClass}>
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
                <button
                  className={styles.fileRemove}
                  onClick={(e) => {
                    e.stopPropagation();
                    onFileRemove(idx);
                  }}
                  aria-label="Remove file"
                >
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                    <path d="M3 3l8 8M11 3l-8 8"/>
                  </svg>
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
