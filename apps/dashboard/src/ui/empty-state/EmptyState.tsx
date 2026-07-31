import styles from "./EmptyState.module.css";

export interface EmptyStateProps {
  title: string;
  description?: React.ReactNode;
  action?: React.ReactNode;
  /** Optional glyph shown above the title. */
  icon?: React.ReactNode;
}

export function EmptyState({ title, description, action, icon }: EmptyStateProps) {
  return (
    <div className={styles.box}>
      {icon && <div className={styles.icon}>{icon}</div>}
      <h3 className={styles.title}>{title}</h3>
      {description && <p className={styles.description}>{description}</p>}
      {action && <div className={styles.action}>{action}</div>}
    </div>
  );
}
