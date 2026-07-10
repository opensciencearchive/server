import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { FilterExpr, FilterPanelData } from "../../lib/types";
import { mockHost } from "../testing";
import { FilterPanel } from "./FilterPanel";

const panel: FilterPanelData = {
  schema: "geo-seq",
  table: "records",
  facets: [
    { field: "metadata.mass", label: "mass", kind: "range", type: "number", unit: "kg" },
    { field: "metadata.collected", label: "collected", kind: "range", type: "date" },
    { field: "metadata.alive", label: "alive", kind: "select", type: "boolean" },
    { field: "metadata.species", label: "species", kind: "select", type: "term" },
    { field: "metadata.notes", label: "notes", kind: "text", type: "text" },
  ],
};

function renderedFilter(): FilterExpr {
  return JSON.parse(screen.getByTestId("filter-json").textContent ?? "null") as FilterExpr;
}

describe("FilterPanel", () => {
  it("renders one control per facet", () => {
    const { host } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    expect(screen.getByLabelText("mass min")).toBeDefined();
    expect(screen.getByLabelText("mass max")).toBeDefined();
    expect(screen.getByLabelText("collected min")).toBeDefined();
    expect(screen.getByLabelText("alive")).toBeDefined();
    expect(screen.getByLabelText("species")).toBeDefined();
    expect(screen.getByLabelText("notes")).toBeDefined();
  });

  it("composes a bare predicate for a single active text facet", async () => {
    const user = userEvent.setup();
    const { host, updateModelContext } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    await user.type(screen.getByLabelText("notes"), "control");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(renderedFilter()).toEqual({
      kind: "predicate",
      field: "metadata.notes",
      op: "contains",
      value: "control",
    });
    expect(updateModelContext).toHaveBeenCalledWith(
      'user set filter: notes contains "control"',
    );
  });

  it("composes a gte/lte pair for a numeric range facet, ANDed", async () => {
    const user = userEvent.setup();
    const { host } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    await user.type(screen.getByLabelText("mass min"), "5");
    await user.type(screen.getByLabelText("mass max"), "10");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(renderedFilter()).toEqual({
      kind: "and",
      operands: [
        { kind: "predicate", field: "metadata.mass", op: "gte", value: 5 },
        { kind: "predicate", field: "metadata.mass", op: "lte", value: 10 },
      ],
    });
  });

  it("keeps date range values as ISO strings", async () => {
    const user = userEvent.setup();
    const { host } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    await user.type(screen.getByLabelText("collected min"), "2026-01-01");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(renderedFilter()).toEqual({
      kind: "predicate",
      field: "metadata.collected",
      op: "gte",
      value: "2026-01-01",
    });
  });

  it("composes an eq predicate with a real boolean for a boolean select", async () => {
    const user = userEvent.setup();
    const { host } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    await user.selectOptions(screen.getByLabelText("alive"), "true");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(renderedFilter()).toEqual({
      kind: "predicate",
      field: "metadata.alive",
      op: "eq",
      value: true,
    });
  });

  it("lazily samples term options with the bare column name", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    callTool.mockResolvedValue({
      column: "species",
      values: ["mouse", "rat"],
      truncated: false,
    });
    render(<FilterPanel data={panel} host={host} />);

    expect(callTool).not.toHaveBeenCalled();
    await user.click(screen.getByLabelText("species"));

    await waitFor(() =>
      expect(callTool).toHaveBeenCalledWith("sample_values", {
        schema: "geo-seq",
        table: "records",
        column: "species",
      }),
    );
    expect(await screen.findByRole("option", { name: "mouse" })).toBeDefined();

    await user.selectOptions(screen.getByLabelText("species"), "mouse");
    await user.click(screen.getByRole("button", { name: "Apply" }));
    expect(renderedFilter()).toEqual({
      kind: "predicate",
      field: "metadata.species",
      op: "eq",
      value: "mouse",
    });
  });

  it("ANDs predicates across multiple facets", async () => {
    const user = userEvent.setup();
    const { host, updateModelContext } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    await user.type(screen.getByLabelText("mass min"), "5");
    await user.type(screen.getByLabelText("notes"), "wild");
    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(renderedFilter()).toEqual({
      kind: "and",
      operands: [
        { kind: "predicate", field: "metadata.mass", op: "gte", value: 5 },
        { kind: "predicate", field: "metadata.notes", op: "contains", value: "wild" },
      ],
    });
    expect(updateModelContext).toHaveBeenCalledWith(
      'user set filter: mass ≥ 5, notes contains "wild"',
    );
  });

  it("reports cleared filters when Apply is hit with no active facet", async () => {
    const user = userEvent.setup();
    const { host, updateModelContext } = mockHost();
    render(<FilterPanel data={panel} host={host} />);

    await user.click(screen.getByRole("button", { name: "Apply" }));

    expect(updateModelContext).toHaveBeenCalledWith("user cleared all filters");
    expect(screen.queryByTestId("filter-json")).toBeNull();
  });
});
