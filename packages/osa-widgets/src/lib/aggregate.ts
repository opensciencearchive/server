/**
 * Client-side chart data preparation. Pure functions over row objects:
 * point extraction (+ optional per-series grouping) for line/scatter,
 * and mean-per-category or equal-width binning for bar charts.
 */

export interface XYPoint {
  x: number | string;
  y: number;
}

export interface PointSeries {
  label: string;
  points: XYPoint[];
}

export interface BarData {
  labels: string[];
  values: number[];
}

const BIN_COUNT = 20;

/** Number() coercion with explicit null/empty skipping (Number(null) === 0). */
function toNumber(value: unknown): number | undefined {
  if (value === null || value === undefined || value === "") return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

export function toPointSeries(
  rows: Record<string, unknown>[],
  x: string,
  y: string,
  opts: { sort: boolean; series?: string },
): PointSeries[] {
  const groups = new Map<string, XYPoint[]>();
  for (const row of rows) {
    const yValue = toNumber(row[y]);
    if (yValue === undefined) continue;
    const xRaw = row[x];
    if (xRaw === null || xRaw === undefined) continue;
    const xValue = toNumber(xRaw) ?? String(xRaw);
    const label = opts.series ? String(row[opts.series] ?? "(none)") : y;
    const points = groups.get(label) ?? [];
    points.push({ x: xValue, y: yValue });
    groups.set(label, points);
  }
  const series = [...groups.entries()].map(([label, points]) => ({ label, points }));
  if (opts.sort) {
    for (const { points } of series) {
      points.sort((a, b) => compareX(a.x, b.x));
    }
  }
  return series;
}

function compareX(a: number | string, b: number | string): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b));
}

/** True when the column has at least one value and every value is numeric. */
export function isNumericColumn(rows: Record<string, unknown>[], column: string): boolean {
  let seen = false;
  for (const row of rows) {
    const value = row[column];
    if (value === null || value === undefined) continue;
    seen = true;
    if (toNumber(value) === undefined) return false;
  }
  return seen;
}

export function toBarData(
  rows: Record<string, unknown>[],
  x: string,
  y: string,
): BarData {
  if (isNumericColumn(rows, x)) return binNumeric(rows, x, y);
  return meanByCategory(rows, x, y);
}

function meanByCategory(
  rows: Record<string, unknown>[],
  x: string,
  y: string,
): BarData {
  const sums = new Map<string, { total: number; count: number }>();
  for (const row of rows) {
    const xRaw = row[x];
    const yValue = toNumber(row[y]);
    if (xRaw === null || xRaw === undefined || yValue === undefined) continue;
    const key = String(xRaw);
    const entry = sums.get(key) ?? { total: 0, count: 0 };
    entry.total += yValue;
    entry.count += 1;
    sums.set(key, entry);
  }
  const labels = [...sums.keys()];
  return { labels, values: labels.map((l) => sums.get(l)!.total / sums.get(l)!.count) };
}

function binNumeric(
  rows: Record<string, unknown>[],
  x: string,
  y: string,
): BarData {
  const pairs: { x: number; y: number }[] = [];
  for (const row of rows) {
    const xValue = toNumber(row[x]);
    const yValue = toNumber(row[y]);
    if (xValue === undefined || yValue === undefined) continue;
    pairs.push({ x: xValue, y: yValue });
  }
  if (pairs.length === 0) return { labels: [], values: [] };

  const xs = pairs.map((p) => p.x);
  const min = Math.min(...xs);
  const max = Math.max(...xs);
  if (min === max) {
    const mean = pairs.reduce((sum, p) => sum + p.y, 0) / pairs.length;
    return { labels: [formatEdge(min)], values: [mean] };
  }

  const width = (max - min) / BIN_COUNT;
  const bins = Array.from({ length: BIN_COUNT }, () => ({ total: 0, count: 0 }));
  for (const pair of pairs) {
    // Clamp so x === max lands in the last bin instead of overflowing.
    const index = Math.min(Math.floor((pair.x - min) / width), BIN_COUNT - 1);
    bins[index]!.total += pair.y;
    bins[index]!.count += 1;
  }
  const labels: string[] = [];
  const values: number[] = [];
  bins.forEach((bin, i) => {
    if (bin.count === 0) return;
    labels.push(`${formatEdge(min + i * width)}–${formatEdge(min + (i + 1) * width)}`);
    values.push(bin.total / bin.count);
  });
  return { labels, values };
}

function formatEdge(n: number): string {
  return Number(n.toPrecision(3)).toString();
}
