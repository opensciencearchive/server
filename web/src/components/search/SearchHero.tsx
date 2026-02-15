import { SearchInput } from './SearchInput';
import styles from './SearchHero.module.css';

export function SearchHero() {
  return (
    <section className={styles.hero}>
      <div className={styles.content}>
        <h1 className={styles.title}>
          Search biology data with <span className={styles.titleAccent}>meaning</span>
        </h1>

        <p className={styles.subtitle}>
          Semantic search over GEO and other biological databases.
          No more keyword guessing, just describe what you&apos;re looking for.
        </p>

        <div className={styles.searchWrapper}>
          <SearchInput
            placeholder="e.g., Alzheimer's neuroinflammatory genes in microglia"
            autoFocus
            size="large"
          />
        </div>

        <div className={styles.examples}>
          <span className={styles.examplesLabel}>Try:</span>
          <div className={styles.exampleChips}>
            <a href="/search?q=single-cell+RNA+sequencing+brain" className={styles.example}>
              single-cell RNA-seq brain
            </a>
            <a href="/search?q=cancer+drug+resistance+mechanisms" className={styles.example}>
              cancer drug resistance
            </a>
            <a href="/search?q=CRISPR+screen+T+cell+activation" className={styles.example}>
              CRISPR screen T cell
            </a>
          </div>
        </div>

      </div>
    </section>
  );
}
