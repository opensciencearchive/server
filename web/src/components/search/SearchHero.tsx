import { SearchInput } from './SearchInput';
import styles from './SearchHero.module.css';

export function SearchHero() {
  return (
    <section className={styles.hero}>
      <div className={styles.content}>
        <h1 className={styles.title}>
          Search GEO with meaning
        </h1>
        <p className={styles.subtitle}>
          Find genomics and transcriptomics datasets using natural language queries.
        </p>

        <div className={styles.searchWrapper}>
          <SearchInput
            placeholder="e.g., Alzheimer's neuroinflammatory genes"
            autoFocus
            size="large"
          />
        </div>

        <div className={styles.examples}>
          <span className={styles.examplesLabel}>Try:</span>
          <a href="/search?q=single-cell+RNA+sequencing+brain" className={styles.example}>
            single-cell RNA sequencing brain
          </a>
          <span className={styles.separator}>/</span>
          <a href="/search?q=cancer+drug+resistance" className={styles.example}>
            cancer drug resistance
          </a>
          <span className={styles.separator}>/</span>
          <a href="/search?q=CRISPR+screen+T+cell" className={styles.example}>
            CRISPR screen T cell
          </a>
        </div>
      </div>
    </section>
  );
}
