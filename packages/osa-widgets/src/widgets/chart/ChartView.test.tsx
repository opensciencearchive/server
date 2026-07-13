import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ChartData, TablePage } from "../../lib/types";

interface FakeChartInstance {
  config: {
    type: string;
    data: { labels?: string[]; datasets: Record<string, unknown>[] };
  };
  destroy: () => void;
}

const { instances } = vi.hoisted(() => ({ instances: [] as FakeChartInstance[] }));

// jsdom has no canvas; mock chart.js entirely and assert on the configs it
// receives instead of on rendered pixels.
vi.mock("chart.js", () => {
  class FakeChart {
    static register = vi.fn();
    destroy = vi.fn();
    constructor(
      public canvas: unknown,
      public config: FakeChartInstance["config"],
    ) {
      instances.push(this as unknown as FakeChartInstance);
    }
  }
  const stub = {};
  return {
    Chart: FakeChart,
    LineController: stub,
    ScatterController: stub,
    BarController: stub,
    LinearScale: stub,
    CategoryScale: stub,
    PointElement: stub,
    LineElement: stub,
    BarElement: stub,
    Tooltip: stub,
    Legend: stub,
  };
});

import { ChartView } from "./ChartView";

function makePage(rows: Record<string, unknown>[], truncated = false): TablePage {
  return {
    query: { schema: "s", table: "t", sort: [], limit: 100 },
    columns: [
      { name: "x", type: "number" },
      { name: "y", type: "number" },
      { name: "grp", type: "term" },
    ],
    rows,
    truncated,
  };
}

function lastInstance(): FakeChartInstance {
  const instance = instances[instances.length - 1];
  if (!instance) throw new Error("Chart was not constructed");
  return instance;
}

describe("ChartView", () => {
  it("renders a line chart with x-sorted points on a linear scale", () => {
    const data: ChartData = {
      kind: "line",
      x: "x",
      y: "y",
      page: makePage([
        { x: 3, y: 30 },
        { x: 1, y: 10 },
        { x: 2, y: 20 },
      ]),
    };
    render(<ChartView data={data} />);

    const { config } = lastInstance();
    expect(config.type).toBe("line");
    expect(config.data.datasets).toHaveLength(1);
    expect(config.data.datasets[0]?.["data"]).toEqual([
      { x: 1, y: 10 },
      { x: 2, y: 20 },
      { x: 3, y: 30 },
    ]);
    expect(config.data.datasets[0]?.["showLine"]).toBe(true);
  });

  it("renders a scatter chart grouped into per-series datasets", () => {
    const data: ChartData = {
      kind: "scatter",
      x: "x",
      y: "y",
      series: "grp",
      page: makePage([
        { x: 1, y: 10, grp: "a" },
        { x: 2, y: 20, grp: "b" },
        { x: 3, y: 30, grp: "a" },
      ]),
    };
    render(<ChartView data={data} />);

    const { config } = lastInstance();
    expect(config.type).toBe("scatter");
    expect(config.data.datasets.map((d) => d["label"])).toEqual(["a", "b"]);
    expect(config.data.datasets[0]?.["showLine"]).toBe(false);
  });

  it("renders a bar chart with mean-per-category aggregation", () => {
    const data: ChartData = {
      kind: "bar",
      x: "grp",
      y: "y",
      page: makePage([
        { grp: "a", y: 10 },
        { grp: "a", y: 20 },
        { grp: "b", y: 5 },
      ]),
    };
    render(<ChartView data={data} />);

    const { config } = lastInstance();
    expect(config.type).toBe("bar");
    expect(config.data.labels).toEqual(["a", "b"]);
    expect(config.data.datasets[0]?.["data"]).toEqual([15, 5]);
  });

  it("shows a prominent truncation notice when the page is truncated", () => {
    const data: ChartData = {
      kind: "line",
      x: "x",
      y: "y",
      page: makePage([{ x: 1, y: 1 }], true),
    };
    render(<ChartView data={data} />);

    expect(
      screen.getByText(/Chart reflects only the first 1 rows — the table is larger\./),
    ).toBeDefined();
  });

  it("omits the truncation notice when the page is complete", () => {
    const data: ChartData = {
      kind: "line",
      x: "x",
      y: "y",
      page: makePage([{ x: 1, y: 1 }]),
    };
    render(<ChartView data={data} />);
    expect(screen.queryByText(/Chart reflects only/)).toBeNull();
  });

  it("destroys the chart instance on unmount", () => {
    const data: ChartData = {
      kind: "bar",
      x: "grp",
      y: "y",
      page: makePage([{ grp: "a", y: 1 }]),
    };
    const { unmount } = render(<ChartView data={data} />);
    const instance = lastInstance();
    unmount();
    expect(instance.destroy).toHaveBeenCalled();
  });
});
