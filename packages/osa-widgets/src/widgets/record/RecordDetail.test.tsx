import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { RecordDetailData, TablePage } from "../../lib/types";
import { mockHost } from "../testing";
import { RecordDetail } from "./RecordDetail";

const detail: RecordDetailData = {
  record: {
    id: "rec-01",
    srn: "urn:osa:node:rec:rec-01@2",
    schema_id: "geo-seq@1.0.0",
    version: 2,
    metadata: { species: "mouse", mass: 12 },
    created_at: "2026-05-01T09:00:00Z",
  },
  schema: "geo-seq",
  feature_tables: ["quality", "alignment"],
};

const featurePage: TablePage = {
  query: { schema: "geo-seq", table: "quality", sort: [], limit: 50 },
  columns: [
    { name: "run_id", type: "text" },
    { name: "score", type: "number" },
  ],
  rows: [{ run_id: "run-77", score: 0.98 }],
  truncated: false,
};

describe("RecordDetail", () => {
  it("renders the record card, srn, schema, version, and metadata grid", () => {
    const { host } = mockHost();
    render(<RecordDetail data={detail} host={host} />);

    expect(screen.getByText("rec-01")).toBeDefined();
    expect(screen.getByText("urn:osa:node:rec:rec-01@2")).toBeDefined();
    expect(screen.getByText("geo-seq@1.0.0")).toBeDefined();
    expect(screen.getByText("2")).toBeDefined();
    expect(screen.getByText("species")).toBeDefined();
    expect(screen.getByText("mouse")).toBeDefined();
  });

  it("renders one collapsed section per feature table without fetching", () => {
    const { host, callTool } = mockHost();
    render(<RecordDetail data={detail} host={host} />);

    expect(screen.getByRole("button", { name: /quality/ })).toBeDefined();
    expect(screen.getByRole("button", { name: /alignment/ })).toBeDefined();
    expect(callTool).not.toHaveBeenCalled();
  });

  it("fetches feature rows on first expand with the record_srn filter", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    callTool.mockResolvedValue(featurePage);
    render(<RecordDetail data={detail} host={host} />);

    await user.click(screen.getByRole("button", { name: /quality/ }));

    await waitFor(() =>
      expect(callTool).toHaveBeenCalledWith("fetch_page", {
        schema: "geo-seq",
        table: "quality",
        sort: [],
        filter: {
          kind: "predicate",
          field: "features.quality.record_srn",
          op: "eq",
          value: "urn:osa:node:rec:rec-01@2",
        },
        limit: 50,
      }),
    );
    await screen.findByText("run-77");
  });

  it("titles the run_id column as the provenance hint 'run'", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    callTool.mockResolvedValue(featurePage);
    render(<RecordDetail data={detail} host={host} />);

    await user.click(screen.getByRole("button", { name: /quality/ }));

    expect(await screen.findByRole("columnheader", { name: "run" })).toBeDefined();
  });

  it("does not refetch when a section is collapsed and reopened", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    callTool.mockResolvedValue(featurePage);
    render(<RecordDetail data={detail} host={host} />);

    const toggle = screen.getByRole("button", { name: /quality/ });
    await user.click(toggle);
    await screen.findByText("run-77");
    await user.click(toggle);
    await user.click(toggle);

    expect(callTool).toHaveBeenCalledTimes(1);
  });

  it("shows a per-section error state when the fetch fails", async () => {
    const user = userEvent.setup();
    const { host, callTool } = mockHost();
    callTool.mockRejectedValue(new Error("no such table"));
    render(<RecordDetail data={detail} host={host} />);

    await user.click(screen.getByRole("button", { name: /quality/ }));

    expect(await screen.findByText(/Failed to load quality: no such table/)).toBeDefined();
  });
});
