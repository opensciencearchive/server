/** Chart.js rendering of a ChartData payload (line / scatter / bar). */

import {
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  LineController,
  LineElement,
  PointElement,
  ScatterController,
  Tooltip,
  type ChartConfiguration,
  type ChartDataset,
} from "chart.js";
import { useEffect, useRef } from "react";

import { isNumericColumn, toBarData, toPointSeries } from "../../lib/aggregate";
import type { ChartData } from "../../lib/types";

// Register only what the three chart kinds need — keeps the bundle lean.
ChartJS.register(
  LineController,
  ScatterController,
  BarController,
  LinearScale,
  CategoryScale,
  PointElement,
  LineElement,
  BarElement,
  Tooltip,
  Legend,
);

const PALETTE = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#b07aa1", "#76b7b2"];

function color(index: number): string {
  return PALETTE[index % PALETTE.length]!;
}

export function buildChartConfig(data: ChartData): ChartConfiguration {
  const rows = data.page.rows;
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: true },
      tooltip: { enabled: true },
    },
    scales: {
      x: { title: { display: true, text: data.x } },
      y: { title: { display: true, text: data.y } },
    },
  };

  if (data.kind === "bar") {
    const bar = toBarData(rows, data.x, data.y);
    return {
      type: "bar",
      data: {
        labels: bar.labels,
        datasets: [{ label: data.y, data: bar.values, backgroundColor: color(0) }],
      },
      options,
    };
  }

  const series = toPointSeries(rows, data.x, data.y, {
    sort: data.kind === "line",
    series: data.series,
  });

  if (isNumericColumn(rows, data.x)) {
    const datasets = series.map((s, i) => ({
      label: s.label,
      data: s.points,
      borderColor: color(i),
      backgroundColor: color(i),
      showLine: data.kind === "line",
    }));
    return {
      type: data.kind,
      data: { datasets },
      options: {
        ...options,
        scales: { ...options.scales, x: { ...options.scales.x, type: "linear" } },
      },
      // Point objects are {x: number, y: number} here; the wide inferred
      // union does not narrow to chart.js's per-type data shape.
    } as ChartConfiguration;
  }

  // Categorical x: align every series onto the union of category labels.
  const labels: string[] = [];
  for (const s of series) {
    for (const point of s.points) {
      const label = String(point.x);
      if (!labels.includes(label)) labels.push(label);
    }
  }
  const datasets: ChartDataset<"line" | "scatter", (number | null)[]>[] = series.map(
    (s, i) => {
      const byLabel = new Map(s.points.map((p) => [String(p.x), p.y]));
      return {
        label: s.label,
        data: labels.map((label) => byLabel.get(label) ?? null),
        borderColor: color(i),
        backgroundColor: color(i),
        showLine: data.kind === "line",
      };
    },
  );
  return { type: data.kind, data: { labels, datasets }, options } as ChartConfiguration;
}

export function ChartView({ data }: { data: ChartData }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const chart = new ChartJS(canvas, buildChartConfig(data));
    return () => chart.destroy();
  }, [data]);

  return (
    <div>
      {data.page.truncated && (
        <div className="notice notice-warning">
          Chart reflects only the first {data.page.rows.length} rows — the table is
          larger.
        </div>
      )}
      <div style={{ position: "relative", height: 360 }}>
        <canvas ref={canvasRef} />
      </div>
    </div>
  );
}
