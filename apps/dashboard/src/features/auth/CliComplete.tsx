import Link from "next/link";

import styles from "./CliComplete.module.css";

/** Terminal page of the `osa login` loopback flow — confirmation only. */
export function CliComplete() {
  return (
    <div className={styles.card}>
      <div className={styles.check} aria-hidden>
        ✓
      </div>
      <p className="eyebrow">Amacrin Cloud</p>
      <h1 className={styles.title}>You&apos;re signed in</h1>
      <p className={styles.copy}>
        The <code>osa</code> CLI has your credentials. You can close this tab
        and return to the terminal.
      </p>
      <pre className={styles.terminal}>
        {`$ osa login
Opening your browser…
✓ Signed in as you — credentials stored.`}
      </pre>
      <Link href="/" className={styles.link}>
        Open the dashboard instead →
      </Link>
    </div>
  );
}
