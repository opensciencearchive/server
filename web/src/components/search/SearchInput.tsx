'use client';

import { useState, useCallback, useEffect, FormEvent } from 'react';
import { useRouter } from 'next/navigation';
import styles from './SearchInput.module.css';

interface SearchInputProps {
  initialQuery?: string;
  placeholder?: string;
  autoFocus?: boolean;
  size?: 'default' | 'large';
}

export function SearchInput({
  initialQuery = '',
  placeholder = 'Search biological datasets...',
  autoFocus = false,
  size = 'default',
}: SearchInputProps) {
  const router = useRouter();
  const [query, setQuery] = useState(initialQuery);

  // Sync local state when URL query changes
  useEffect(() => {
    setQuery(initialQuery);
  }, [initialQuery]);

  const handleSubmit = useCallback(
    (e: FormEvent) => {
      e.preventDefault();
      const trimmedQuery = query.trim();
      if (trimmedQuery) {
        router.push(`/search?q=${encodeURIComponent(trimmedQuery)}`);
      }
    },
    [query, router]
  );

  return (
    <form
      className={`${styles.form} ${size === 'large' ? styles.large : ''}`}
      onSubmit={handleSubmit}
    >
      <div className={styles.inputWrapper}>
        <svg
          className={styles.searchIcon}
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <input
          type="text"
          className={styles.input}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={placeholder}
          autoFocus={autoFocus}
          aria-label="Search query"
        />
        {query && (
          <button
            type="button"
            className={styles.clearButton}
            onClick={() => setQuery('')}
            aria-label="Clear search"
          >
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        )}
      </div>
      {size === 'large' && (
        <button
          type="submit"
          className={styles.submitButton}
          disabled={!query.trim()}
        >
          Search
        </button>
      )}
    </form>
  );
}
