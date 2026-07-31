import styles from "./PageHeader.module.css";

export interface PageHeaderProps {
  title: string;
  description?: React.ReactNode;
  actions?: React.ReactNode;
  /** Optional leading glyph (e.g. a nav icon) shown beside the title. */
  icon?: React.ReactNode;
}

export function PageHeader({
  title,
  description,
  actions,
  icon,
}: PageHeaderProps) {
  return (
    <header className={styles.header}>
      <div>
        <div className={styles.titleRow}>
          {icon && <span className={styles.icon}>{icon}</span>}
          <h1 className={styles.title}>{title}</h1>
        </div>
        {description && <p className={styles.description}>{description}</p>}
      </div>
      {actions && <div className={styles.actions}>{actions}</div>}
    </header>
  );
}
