import styles from "./BarChart.module.css";

export interface BarChartProps {
  points: Array<{ label: string; value: number }>;
  /** Accessible description of the chart. */
  title: string;
  /** Annotation shown top-right, e.g. "peak 612 · Mar". */
  annotation?: string;
}

/** Minimal monthly bar chart in the mockup's style — pure CSS bars. */
export function BarChart({ points, title, annotation }: BarChartProps) {
  const max = Math.max(1, ...points.map((p) => p.value));
  const first = points[0];
  const last = points[points.length - 1];

  return (
    <figure className={styles.figure} role="img" aria-label={title}>
      {annotation && <div className={styles.annotation}>{annotation}</div>}
      <div className={styles.bars}>
        {points.map((point) => (
          <div
            key={point.label}
            className={styles.bar}
            style={{ height: `${Math.round((point.value / max) * 100)}%` }}
            title={`${point.label}: ${point.value.toLocaleString("en-GB")}`}
          />
        ))}
      </div>
      <figcaption className={styles.axis}>
        <span>{first?.label}</span>
        <span>{last?.label}</span>
      </figcaption>
    </figure>
  );
}
