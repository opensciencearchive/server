import styles from "./Stat.module.css";

export interface StatProps {
  label: string;
  value: React.ReactNode;
  /** Small note under the value, e.g. "+318 this month". */
  note?: React.ReactNode;
  noteTone?: "neutral" | "success";
}

export function Stat({ label, value, note, noteTone = "neutral" }: StatProps) {
  return (
    <div className={styles.stat}>
      <div className={styles.label}>{label}</div>
      <div className={styles.value}>{value}</div>
      {note && (
        <div
          className={[
            styles.note,
            noteTone === "success" ? styles.noteSuccess : "",
          ].join(" ")}
        >
          {note}
        </div>
      )}
    </div>
  );
}
