import { Suspense } from 'react';
import { redirect } from 'next/navigation';
import { osa } from '@/lib/sdk';
import { SearchInput } from '@/components/search/SearchInput';
import { SearchResults } from '@/components/search/SearchResults';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ErrorMessage } from '@/components/ui/ErrorMessage';
import { isApiError } from '@/types';
import styles from './page.module.css';

interface SearchPageProps {
  searchParams: Promise<{ q?: string }>;
}

async function SearchResultsLoader({ query }: { query: string }) {
  let data;
  let errorMessage: string | null = null;

  try {
    data = await osa.search.query(query);
  } catch (error) {
    errorMessage = isApiError(error)
      ? error.detail
      : 'Failed to fetch search results. Please try again.';
  }

  if (errorMessage) {
    return <ErrorMessage title="Search failed" message={errorMessage} />;
  }

  return <SearchResults initialData={data!} />;
}

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const params = await searchParams;
  const query = params.q?.trim();

  if (!query) {
    redirect('/');
  }

  return (
    <main className={styles.main}>
      <div className={styles.toolbar}>
        <div className={styles.searchWrapper}>
          <SearchInput initialQuery={query} />
        </div>
      </div>

      <div className={styles.content}>
        <Suspense fallback={<LoadingSpinner size="md" label="Searching..." />}>
          <SearchResultsLoader query={query} />
        </Suspense>
      </div>
    </main>
  );
}
