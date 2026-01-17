'use client';

import { useState, useCallback } from 'react';
import type { SearchResponse, SearchHit as SearchHitType } from '@/types';
import { api } from '@/lib/api';
import { SearchHit } from './SearchHit';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { EmptyState } from '@/components/ui/EmptyState';
import styles from './SearchResults.module.css';

interface SearchResultsProps {
  initialData: SearchResponse;
}

export function SearchResults({ initialData }: SearchResultsProps) {
  const [results, setResults] = useState<SearchHitType[]>(initialData.results);
  const [hasMore, setHasMore] = useState(initialData.has_more);
  const [loading, setLoading] = useState(false);

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
      <EmptyState
        title="No results found"
        message={`We couldn't find any records matching "${initialData.query}"`}
        suggestion="Try using different keywords or broader search terms"
      />
    );
  }

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <p className={styles.count}>
          Showing {results.length} of {initialData.total.toLocaleString()} results
        </p>
      </div>

      <div className={styles.list}>
        {results.map((hit) => (
          <SearchHit key={hit.srn} hit={hit} />
        ))}
      </div>

      {hasMore && (
        <div className={styles.loadMore}>
          {loading ? (
            <LoadingSpinner size="sm" label="Loading more..." />
          ) : (
            <button className={styles.loadMoreButton} onClick={loadMore}>
              Load more results
            </button>
          )}
        </div>
      )}
    </div>
  );
}
