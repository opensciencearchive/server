"use client";

import Link from "next/link";

import { DOCS_URL } from "../shell/links";
import styles from "./tenant-insights.module.css";

interface Step {
  title: string;
  description: React.ReactNode;
}

const STEPS: Step[] = [
  {
    title: "Update the convention",
    description: (
      <>
        New checks or features go live with <code>osa deploy</code>
      </>
    ),
  },
  {
    title: "Invite depositors",
    description: "Share the deposit link; they sign in with ORCID",
  },
  {
    title: "Export a training set",
    description: "Validated records plus features, pinned to a digest",
  },
  {
    title: "Cite this archive",
    description: "Stable SRNs for the paper's data availability statement",
  },
];

export function NextSteps() {
  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <h3>Next steps</h3>
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
        {STEPS.map((step) => (
          <Link key={step.title} href="./records" className={styles.stepCard}>
            <span className={styles.stepTitle}>{step.title}</span>
            <span className={styles.stepDesc}>{step.description}</span>
          </Link>
        ))}
      </div>
    </section>
  );
}
