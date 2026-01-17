import { Suspense } from 'react';
import { redirect } from 'next/navigation';
import { api } from '@/lib/api';
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
  try {
    const data = await api.search(query);
    return <SearchResults initialData={data} />;
  } catch (error) {
    const message = isApiError(error)
      ? error.detail
      : 'Failed to fetch search results. Please try again.';
    return <ErrorMessage title="Search failed" message={message} />;
  }
}

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const params = await searchParams;
  const query = params.q?.trim();

  if (!query) {
    redirect('/');
  }

  return (
    <main className={styles.main}>
      <div className={styles.container}>
        <header className={styles.searchHeader}>
          <SearchInput initialQuery={query} />
        </header>

        <Suspense fallback={<LoadingSpinner size="md" label="Searching..." />}>
          <SearchResultsLoader query={query} />
        </Suspense>
      </div>
    </main>
  );
}
