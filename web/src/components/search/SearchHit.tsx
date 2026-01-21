'use client';

import Link from 'next/link';
import type { SearchHit as SearchHitType } from '@/types';
import {
  formatScore,
  getDisplaySrn,
  formatOrganism,
} from '@/lib/utils/format';
import styles from './SearchHit.module.css';

interface SearchHitProps {
  hit: SearchHitType;
  index: number;
  isFocused?: boolean;
  isPinned?: boolean;
  onFocus?: () => void;
  onTogglePin?: () => void;
}

export function SearchHit({
  hit,
  index,
  isFocused = false,
  isPinned = false,
  onFocus,
  onTogglePin,
}: SearchHitProps) {
  const { srn, score, metadata } = hit;

  const handleClick = (e: React.MouseEvent) => {
    // On desktop, clicking focuses the row (shows detail in panel)
    if (onFocus && window.innerWidth >= 900) {
      e.preventDefault();
      onFocus();
    }
  };

  const handlePinClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    onTogglePin?.();
  };

  const rowClasses = [
    styles.row,
    isFocused ? styles.focused : '',
    isPinned ? styles.pinned : '',
  ].filter(Boolean).join(' ');

  return (
    <Link
      href={`/record/${encodeURIComponent(srn)}`}
      className={rowClasses}
      onClick={handleClick}
      data-index={index}
      data-focused={isFocused}
    >
      <button
        type="button"
        className={`${styles.pinButton} ${isPinned ? styles.pinButtonActive : ''}`}
        onClick={handlePinClick}
        aria-label={isPinned ? 'Unpin from comparison' : 'Pin for comparison'}
      >
        <svg viewBox="0 0 16 16" className={styles.pinIcon}>
          <path
            d="M9.5 1.5L14.5 6.5L10 11L11 15L6 10L1.5 14.5M9.5 1.5L6 5M9.5 1.5L11.5 3.5"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>
      <div className={styles.content}>
        <div className={styles.title}>{metadata.title}</div>
        <div className={styles.meta}>
          <code className={styles.srn}>{getDisplaySrn(srn)}</code>
          <span className={styles.separator}>·</span>
          <span>{formatOrganism(metadata.organism)}</span>
        </div>
      </div>
      <span className={styles.score}>{formatScore(score)}</span>
    </Link>
  );
}
