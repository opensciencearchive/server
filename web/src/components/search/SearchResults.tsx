'use client';

import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import type { SearchResponse, SearchHit as SearchHitType, Record } from '@/types';
import { api } from '@/lib/api';
import { SearchHit } from './SearchHit';
import { RecordDetail } from '@/components/record/RecordDetail';
import { RecordComparison } from '@/components/record/RecordComparison';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { EmptyState } from '@/components/ui/EmptyState';
import styles from './SearchResults.module.css';

interface SearchResultsProps {
  initialData: SearchResponse;
}

// Convert SearchHit to Record (they have the same shape minus score)
function hitToRecord(hit: SearchHitType): Record {
  return { srn: hit.srn, metadata: hit.metadata };
}

export function SearchResults({ initialData }: SearchResultsProps) {
  const [results, setResults] = useState<SearchHitType[]>(initialData.results);
  const [hasMore, setHasMore] = useState(initialData.has_more);
  const [loading, setLoading] = useState(false);
  const [focusedIndex, setFocusedIndex] = useState<number>(0);
  const [pinnedSrn, setPinnedSrn] = useState<string | null>(null);

  const listRef = useRef<HTMLDivElement>(null);

  // Sync state when search results change (new query)
  useEffect(() => {
    setResults(initialData.results);
    setHasMore(initialData.has_more);
    setFocusedIndex(0);
    setPinnedSrn(null);
  }, [initialData]);

  // Index results by SRN for O(1) lookup
  const recordsBySrn = useMemo(() => {
    const map = new Map<string, Record>();
    for (const hit of results) {
      map.set(hit.srn, hitToRecord(hit));
    }
    return map;
  }, [results]);

  const focusedHit = results[focusedIndex] ?? null;
  const selectedRecord = focusedHit ? recordsBySrn.get(focusedHit.srn) ?? null : null;
  const pinnedRecord = pinnedSrn ? recordsBySrn.get(pinnedSrn) ?? null : null;
  const isComparing = pinnedSrn !== null && focusedHit !== null && focusedHit.srn !== pinnedSrn;

  const togglePin = useCallback((srn: string | undefined) => {
    if (!srn) return;
    setPinnedSrn(prev => prev === srn ? null : srn);
  }, []);

  // Keyboard navigation: j/k to move, space to pin/unpin for comparison
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Don't handle if user is typing in an input
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
        return;
      }

      if (e.key === 'j' || e.key === 'ArrowDown') {
        e.preventDefault();
        setFocusedIndex(prev => Math.min(prev + 1, results.length - 1));
      } else if (e.key === 'k' || e.key === 'ArrowUp') {
        e.preventDefault();
        setFocusedIndex(prev => Math.max(prev - 1, 0));
      } else if (e.key === ' ') {
        e.preventDefault();
        togglePin(focusedHit?.srn);
      } else if (e.key === 'Escape' && pinnedSrn) {
        e.preventDefault();
        setPinnedSrn(null);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [results.length, focusedHit?.srn, pinnedSrn, togglePin]);

  // Scroll focused item into view
  useEffect(() => {
    const list = listRef.current;
    if (!list) return;

    const focusedElement = list.querySelector(`[data-index="${focusedIndex}"]`);
    if (focusedElement) {
      focusedElement.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, [focusedIndex]);

  const loadMore = useCallback(async () => {
    if (loading || !hasMore) return;

    setLoading(true);
    try {
      const response = await api.search(initialData.query, initialData.index, {
        offset: results.length,
      });
      setResults((prev) => [...prev, ...response.results]);
      setHasMore(response.has_more);
    } catch (error) {
      console.error('Failed to load more results:', error);
    } finally {
      setLoading(false);
    }
  }, [loading, hasMore, results.length, initialData.query, initialData.index]);

  if (results.length === 0) {
    return (
      <div className={styles.container}>
        <EmptyState
          title="No results found"
          message={`No records matching "${initialData.query}"`}
          suggestion="Try different keywords or broader search terms"
        />
      </div>
    );
  }

  const comparisonReady = isComparing && pinnedRecord && selectedRecord;

  return (
    <div className={styles.container}>
      {/* Sidebar with results list */}
      <div className={styles.sidebar}>
        <div className={styles.sidebarHeader}>
          <div className={styles.sidebarHeaderLeft}>
            <span>Results</span>
            <span className={styles.count}>{initialData.total.toLocaleString()}</span>
          </div>
          <span className={styles.keyHint}>
            <kbd>j</kbd><kbd>k</kbd> navigate · <kbd>space</kbd> pin
          </span>
        </div>

        <div className={styles.list} ref={listRef}>
          {results.map((hit, index) => (
            <SearchHit
              key={hit.srn}
              hit={hit}
              index={index}
              isFocused={index === focusedIndex}
              isPinned={hit.srn === pinnedSrn}
              onFocus={() => setFocusedIndex(index)}
              onTogglePin={() => togglePin(hit.srn)}
            />
          ))}
        </div>

        {hasMore && (
          <div className={styles.loadMore}>
            {loading ? (
              <LoadingSpinner size="sm" />
            ) : (
              <button className={styles.loadMoreButton} onClick={loadMore}>
                Load more results
              </button>
            )}
          </div>
        )}
      </div>

      {/* Main content - record detail or comparison */}
      <div className={styles.main}>
        <div className={styles.mainHeader}>
          {pinnedSrn ? (
            <>
              <span>{isComparing ? 'Comparing' : 'Pinned'}</span>
              <button
                className={styles.exitCompareButton}
                onClick={() => setPinnedSrn(null)}
              >
                Unpin <kbd>esc</kbd>
              </button>
            </>
          ) : (
            <span>Record Detail</span>
          )}
        </div>

        <div className={styles.mainContent}>
          {comparisonReady ? (
            <RecordComparison records={[pinnedRecord, selectedRecord]} />
          ) : selectedRecord ? (
            <RecordDetail record={selectedRecord} />
          ) : (
            <div className={styles.emptyState}>
              Select a record to view details
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
