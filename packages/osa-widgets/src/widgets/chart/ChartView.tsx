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

// Sage-anchored categorical palette (OSA brand): muted, earthy hues that sit
// alongside the sage primary and read clearly on the off-white surface.
const SERIES_HSL: ReadonlyArray<readonly [number, number, number]> = [
  [160, 30, 42], // sage
  [24, 42, 52], // terracotta
  [42, 44, 46], // ochre
  [205, 32, 48], // dusty blue
  [280, 18, 55], // muted mauve
  [190, 32, 40], // deep teal
];

function hsl([h, s, l]: readonly [number, number, number], alpha = 1): string {
  return alpha < 1 ? `hsla(${h}, ${s}%, ${l}%, ${alpha})` : `hsl(${h}, ${s}%, ${l}%)`;
}

function seriesColor(index: number, alpha = 1): string {
  return hsl(SERIES_HSL[index % SERIES_HSL.length]!, alpha);
}

/** Read a CSS custom property (falling back for non-browser test envs). */
function token(name: string, fallback: string): string {
  if (typeof getComputedStyle !== "function" || typeof document === "undefined") return fallback;
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

/** Prettify a raw column name for an axis title: ``max_od_avg`` → ``max od avg``. */
function axisTitle(name: string): string {
  return name.replace(/_/g, " ");
}

const FONT_FAMILY =
  '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';

// Guarded: unit tests mock chart.js, so `defaults` may be absent.
if (ChartJS.defaults?.font) {
  ChartJS.defaults.font.family = FONT_FAMILY;
  ChartJS.defaults.font.size = 11;
}

/** Axis/legend/tooltip styling shared by all three chart kinds. */
function baseOptions(data: ChartData): Record<string, unknown> {
  const fg = token("--fg", "#0d0d0d");
  const muted = token("--fg-muted", "#616161");
  const border = token("--border", "#e0e0e0");
  const surface = token("--bg", "#fdfdfd");
  const multiSeries = data.series != null;

  const axis = (title: string) => ({
    title: {
      display: true,
      text: axisTitle(title),
      color: muted,
      font: { size: 12, weight: 500 },
    },
    ticks: { color: muted },
    grid: { color: border, drawTicks: false },
    border: { color: border },
  });

  return {
    responsive: true,
    maintainAspectRatio: false,
    layout: { padding: 4 },
    plugins: {
      // A single series needs no legend — the axis titles already say what it is.
      legend: {
        display: multiSeries,
        position: "bottom",
        labels: { color: muted, boxWidth: 10, boxHeight: 10, usePointStyle: true, padding: 12 },
      },
      tooltip: {
        backgroundColor: fg,
        titleColor: surface,
        bodyColor: surface,
        borderColor: border,
        borderWidth: 0,
        padding: 8,
        cornerRadius: 6,
        displayColors: multiSeries,
      },
    },
    scales: { x: axis(data.x), y: axis(data.y) },
  };
}

export function buildChartConfig(data: ChartData): ChartConfiguration {
  const rows = data.page.rows;
  const options = baseOptions(data);

  if (data.kind === "bar") {
    const bar = toBarData(rows, data.x, data.y);
    return {
      type: "bar",
      data: {
        labels: bar.labels,
        datasets: [
          {
            label: axisTitle(data.y),
            data: bar.values,
            backgroundColor: seriesColor(0, 0.8),
            hoverBackgroundColor: seriesColor(0),
            borderColor: seriesColor(0),
            borderWidth: 1,
            borderRadius: 3,
            maxBarThickness: 46,
          },
        ],
      },
      options,
    };
  }

  const series = toPointSeries(rows, data.x, data.y, {
    sort: data.kind === "line",
    series: data.series,
  });
  const isLine = data.kind === "line";
  // A lone line reads better with a soft area fill; overlapping fills muddy a
  // multi-series chart, so only fill when there is exactly one line.
  const fillSingleLine = isLine && series.length === 1;

  const styleDataset = (index: number) => ({
    borderColor: seriesColor(index),
    backgroundColor: fillSingleLine ? seriesColor(index, 0.12) : seriesColor(index),
    pointBackgroundColor: seriesColor(index),
    pointBorderColor: seriesColor(index),
    pointRadius: isLine ? 2 : 3,
    pointHoverRadius: 5,
    borderWidth: 2,
    tension: 0.35,
    showLine: isLine,
    fill: fillSingleLine ? "origin" : false,
  });

  if (isNumericColumn(rows, data.x)) {
    const datasets = series.map((s, i) => ({
      label: s.label,
      data: s.points,
      ...styleDataset(i),
    }));
    return {
      type: data.kind,
      data: { datasets },
      options: {
        ...options,
        scales: {
          ...(options.scales as Record<string, unknown>),
          x: { ...(options.scales as Record<string, { x: unknown }>).x, type: "linear" },
        },
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
        ...styleDataset(i),
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
