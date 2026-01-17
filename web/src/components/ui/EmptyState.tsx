import styles from './EmptyState.module.css';

interface EmptyStateProps {
  title: string;
  message: string;
  suggestion?: string;
}

export function EmptyState({ title, message, suggestion }: EmptyStateProps) {
  return (
    <div className={styles.container}>
      <h3 className={styles.title}>{title}</h3>
      <p className={styles.message}>{message}</p>
      {suggestion && <p className={styles.suggestion}>{suggestion}</p>}
    </div>
  );
}
