"use client";

import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";

import styles from "./agent.module.css";

/**
 * Render the archive's SKILL.md. react-markdown does not emit raw HTML, so the
 * server-authored markdown is displayed safely; the `.prose` wrapper styles the
 * standard elements (headings, tables, code).
 */
export function SkillSheet({ markdown }: { markdown: string }) {
  return (
    <div className={styles.sheet}>
      <div className={styles.prose}>
        <Markdown remarkPlugins={[remarkGfm]}>{markdown}</Markdown>
      </div>
    </div>
  );
}
