"use client";

import Link from "next/link";

import { icons } from "../shell/icons";
import { DOCS_URL } from "../shell/links";
import styles from "./tenant-insights.module.css";

interface Step {
  title: string;
  description: React.ReactNode;
  icon: React.ReactNode;
  href: string;
  external?: boolean;
}

function steps(base: string): Step[] {
  return [
    {
      title: "Update the convention",
      description: (
        <>
          New checks or features go live with <code>osa deploy</code>
        </>
      ),
      icon: icons.hooks,
      href: DOCS_URL,
      external: true,
    },
    {
      title: "Invite depositors",
      description: "Share the deposit link; they sign in with ORCID",
      icon: icons.authentication,
      href: `${base}/authentication`,
    },
    {
      title: "Export a training set",
      description: "Validated records plus features, pinned to a digest",
      icon: icons.ingesters,
      href: `${base}/records`,
    },
    {
      title: "Publish to agents",
      description: "A skill file and MCP server language models can query",
      icon: icons.agents,
      href: `${base}/agents`,
    },
  ];
}

export function NextSteps({ archiveId }: { archiveId: string }) {
  const base = `/archives/${archiveId}`;

  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <span className={styles.sectionTitle}>
          <span className={styles.sectionIcon}>{icons.overview}</span>
          <h3>Next steps</h3>
        </span>
        <a
          className={styles.headLink}
          href={DOCS_URL}
          target="_blank"
          rel="noreferrer"
        >
          Documentation →
        </a>
      </div>

      <div className={styles.stepGrid}>
        {steps(base).map((step) => (
          <Link
            key={step.title}
            href={step.href}
            className={styles.stepCard}
            {...(step.external
              ? { target: "_blank", rel: "noreferrer" }
              : {})}
          >
            <span className={styles.stepIcon}>{step.icon}</span>
            <span className={styles.stepTitle}>{step.title}</span>
            <span className={styles.stepDesc}>{step.description}</span>
          </Link>
        ))}
      </div>
    </section>
  );
}
