import { describe, expect, it } from "vitest";

import { isNumericColumn, toBarData, toPointSeries } from "./aggregate";

const rows = (pairs: [unknown, unknown][]): Record<string, unknown>[] =>
  pairs.map(([x, y]) => ({ x, y }));

describe("toPointSeries", () => {
  it("maps rows to {x, y} points", () => {
    const series = toPointSeries(rows([[1, 10], [2, 20]]), "x", "y", { sort: false });
    expect(series).toEqual([{ label: "y", points: [{ x: 1, y: 10 }, { x: 2, y: 20 }] }]);
  });

  it("sorts points by x when asked (line charts)", () => {
    const series = toPointSeries(rows([[3, 30], [1, 10], [2, 20]]), "x", "y", { sort: true });
    expect(series[0]?.points.map((p) => p.x)).toEqual([1, 2, 3]);
  });

  it("groups into per-series datasets by a categorical column", () => {
    const data = [
      { x: 1, y: 10, grp: "a" },
      { x: 2, y: 20, grp: "b" },
      { x: 3, y: 30, grp: "a" },
    ];
    const series = toPointSeries(data, "x", "y", { sort: false, series: "grp" });
    expect(series).toHaveLength(2);
    expect(series.find((s) => s.label === "a")?.points).toEqual([
      { x: 1, y: 10 },
      { x: 3, y: 30 },
    ]);
    expect(series.find((s) => s.label === "b")?.points).toEqual([{ x: 2, y: 20 }]);
  });

  it("coerces numeric strings via Number()", () => {
    const series = toPointSeries(rows([["1", "10.5"]]), "x", "y", { sort: false });
    expect(series[0]?.points).toEqual([{ x: 1, y: 10.5 }]);
  });

  it("skips rows with null or non-numeric y", () => {
    const series = toPointSeries(
      rows([[1, null], [2, "oops"], [3, 30]]),
      "x",
      "y",
      { sort: false },
    );
    expect(series[0]?.points).toEqual([{ x: 3, y: 30 }]);
  });

  it("keeps non-numeric x values as category strings", () => {
    const series = toPointSeries(rows([["mouse", 5]]), "x", "y", { sort: false });
    expect(series[0]?.points).toEqual([{ x: "mouse", y: 5 }]);
  });

  it("returns an empty array for empty rows", () => {
    expect(toPointSeries([], "x", "y", { sort: true })).toEqual([]);
  });
});

describe("isNumericColumn", () => {
  it("is true when all non-null values coerce to finite numbers", () => {
    expect(isNumericColumn(rows([[1, 0], ["2.5", 0], [null, 0]]), "x")).toBe(true);
  });

  it("is false when any value fails coercion", () => {
    expect(isNumericColumn(rows([[1, 0], ["mouse", 0]]), "x")).toBe(false);
  });

  it("is false when there are no values at all", () => {
    expect(isNumericColumn([], "x")).toBe(false);
    expect(isNumericColumn(rows([[null, 0]]), "x")).toBe(false);
  });
});

describe("toBarData (categorical x)", () => {
  it("aggregates y by mean per category", () => {
    const data = rows([
      ["a", 10],
      ["a", 20],
      ["b", 5],
    ]);
    expect(toBarData(data, "x", "y")).toEqual({ labels: ["a", "b"], values: [15, 5] });
  });

  it("skips null and non-numeric y values inside a category", () => {
    const data = rows([
      ["a", 10],
      ["a", null],
      ["a", "junk"],
    ]);
    expect(toBarData(data, "x", "y")).toEqual({ labels: ["a"], values: [10] });
  });
});

describe("toBarData (numeric x binning)", () => {
  it("returns empty data for empty rows", () => {
    expect(toBarData([], "x", "y")).toEqual({ labels: [], values: [] });
  });

  it("puts a single x value into a single bin", () => {
    const data = rows([[5, 10], [5, 30]]);
    const result = toBarData(data, "x", "y");
    expect(result.labels).toHaveLength(1);
    expect(result.values).toEqual([20]);
  });

  it("bins numeric x into at most ~20 equal-width bins with mean y", () => {
    const data = rows(
      Array.from({ length: 100 }, (_, i) => [i, i * 2] as [unknown, unknown]),
    );
    const result = toBarData(data, "x", "y");
    expect(result.labels.length).toBeLessThanOrEqual(20);
    expect(result.labels.length).toBeGreaterThan(1);
    // Bin means of y = 2x must be monotonically increasing.
    for (let i = 1; i < result.values.length; i++) {
      expect(result.values[i]!).toBeGreaterThan(result.values[i - 1]!);
    }
  });

  it("places the maximum x value in the last bin, not out of range", () => {
    const data = rows([[0, 1], [100, 3]]);
    const result = toBarData(data, "x", "y");
    expect(result.values.reduce((a, b) => a + b, 0)).toBe(4);
  });

  it("skips rows with null x or y and coerces numeric strings", () => {
    const data = rows([[null, 5], ["10", "7"], [20, null]]);
    const result = toBarData(data, "x", "y");
    expect(result.values).toEqual([7]);
  });
});
