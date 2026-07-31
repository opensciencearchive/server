import Link from "next/link";

import { CenteredScreen } from "./CenteredScreen";
import styles from "./CenteredNotice.module.css";

export interface CenteredNoticeProps {
  title: string;
  description?: React.ReactNode;
  /** Optional CTA rendered as a button-styled link (e.g. back to the fleet). */
  actionHref?: string;
  actionLabel?: string;
  /** `danger` tints the icon badge for failures; `default` is neutral. */
  tone?: "default" | "danger";
  /** Glyph shown in the badge above the title. Defaults to a neutral mark. */
  icon?: React.ReactNode;
}

/**
 * A full-page centered card for standalone terminal states — archive not found,
 * deployment failed. Chrome-less (renders no `AppShell`), so it can replace a
 * page or short-circuit a layout.
 */
export function CenteredNotice({
  title,
  description,
  actionHref,
  actionLabel,
  tone = "default",
  icon,
}: CenteredNoticeProps) {
  const badgeClass = tone === "danger" ? `${styles.badge} ${styles.danger}` : styles.badge;
  return (
    <CenteredScreen>
      <div className={styles.card}>
        <div className={badgeClass} aria-hidden>
          {icon ?? (tone === "danger" ? "!" : "?")}
        </div>
        <h1 className={styles.title}>{title}</h1>
        {description && <p className={styles.description}>{description}</p>}
        {actionHref && actionLabel && (
          <Link className={styles.action} href={actionHref}>
            {actionLabel}
          </Link>
        )}
      </div>
    </CenteredScreen>
  );
}
