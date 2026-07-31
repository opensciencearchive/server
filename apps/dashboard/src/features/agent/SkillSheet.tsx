"use client";

import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { CopyButton } from "@/ui";

import styles from "./agent.module.css";

interface Frontmatter {
  name?: string;
  description?: string;
}

/**
 * Split a leading `--- … ---` YAML frontmatter block from the markdown body.
 * SKILL.md frontmatter is a flat `key: value` map (values may wrap onto
 * continuation lines), so a minimal parser is enough — we only surface `name`
 * and `description`.
 */
function splitFrontmatter(markdown: string): {
  frontmatter: Frontmatter | null;
  body: string;
} {
  if (!markdown.startsWith("---")) return { frontmatter: null, body: markdown };
  const end = markdown.indexOf("\n---", 3);
  if (end === -1) return { frontmatter: null, body: markdown };

  const raw = markdown.slice(3, end).trim();
  const body = markdown.slice(end + 4).replace(/^\s*\n/, "");

  const fm: Record<string, string> = {};
  let key: string | null = null;
  for (const line of raw.split("\n")) {
    const match = /^([A-Za-z0-9_-]+):\s*(.*)$/.exec(line);
    if (match) {
      key = match[1]!;
      fm[key] = match[2]!;
    } else if (key && line.trim()) {
      fm[key] = `${fm[key]} ${line.trim()}`.trim();
    }
  }
  return { frontmatter: { name: fm.name, description: fm.description }, body };
}

/**
 * Render the archive's SKILL.md: a header bar with a copy-raw action, the
 * frontmatter (name + description) in a labelled block, then the body. Markdown
 * is rendered without raw HTML, so the server-authored text is displayed safely.
 */
export function SkillSheet({ markdown }: { markdown: string }) {
  const { frontmatter, body } = splitFrontmatter(markdown);

  return (
    <section>
      <div className={styles.sectionHead}>
        <h3>SKILL.md</h3>
        <CopyButton value={markdown} label="Copy raw" size="sm" />
      </div>
      <div className={styles.sheet}>
        <div className={styles.sheetBody}>
          {frontmatter && (frontmatter.name || frontmatter.description) && (
            <div className={styles.frontmatter}>
              <span className={styles.fmLabel}>Frontmatter</span>
              {frontmatter.name && (
                <div className={styles.fmRow}>
                  <span className={styles.fmKey}>name</span>
                  <span className={styles.fmVal}>{frontmatter.name}</span>
                </div>
              )}
              {frontmatter.description && (
                <div className={styles.fmRow}>
                  <span className={styles.fmKey}>description</span>
                  <span className={styles.fmVal}>{frontmatter.description}</span>
                </div>
              )}
            </div>
          )}
          <div className={styles.prose}>
            <Markdown remarkPlugins={[remarkGfm]}>{body}</Markdown>
          </div>
        </div>
      </div>
    </section>
  );
}
