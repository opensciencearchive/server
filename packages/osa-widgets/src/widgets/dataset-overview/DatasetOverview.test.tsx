import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { DatasetList } from "../../lib/types";
import { mockHost } from "../testing";
import { DatasetOverview } from "./DatasetOverview";

const list: DatasetList = {
  node_domain: "archive.university.edu",
  datasets: [
    {
      id: "geo-seq",
      version: "1.2.0",
      srn: "urn:osa:archive.university.edu:schema:geo-seq@1.2.0",
      title: "GEO Sequencing Runs",
      record_count: 15423,
      feature_tables: ["quality", "alignment"],
    },
    {
      id: "admet",
      version: "0.3.1",
      srn: "urn:osa:archive.university.edu:schema:admet@0.3.1",
      title: "ADMET Assays",
      record_count: 0,
      feature_tables: [],
    },
  ],
};

describe("DatasetOverview", () => {
  it("renders the node domain and one card per dataset", () => {
    const { host } = mockHost();
    render(<DatasetOverview data={list} host={host} />);

    expect(screen.getByText(/archive\.university\.edu/)).toBeDefined();
    expect(screen.getByText("GEO Sequencing Runs")).toBeDefined();
    expect(screen.getByText("ADMET Assays")).toBeDefined();
    expect(screen.getByText(/geo-seq@1\.2\.0/)).toBeDefined();
    expect(screen.getByText(new RegExp(`${(15423).toLocaleString()} records`))).toBeDefined();
  });

  it("renders feature-table chips", () => {
    const { host } = mockHost();
    render(<DatasetOverview data={list} host={host} />);

    expect(screen.getByText("quality")).toBeDefined();
    expect(screen.getByText("alignment")).toBeDefined();
  });

  it("updates model context when a dataset card is clicked", async () => {
    const user = userEvent.setup();
    const { host, updateModelContext } = mockHost();
    render(<DatasetOverview data={list} host={host} />);

    await user.click(screen.getByText("GEO Sequencing Runs"));

    expect(updateModelContext).toHaveBeenCalledWith(
      "user selected dataset geo-seq — show its records table",
    );
  });

  it("shows an empty state for zero datasets", () => {
    const { host } = mockHost();
    render(
      <DatasetOverview data={{ node_domain: "localhost", datasets: [] }} host={host} />,
    );

    expect(screen.getByText("No published datasets on this node yet.")).toBeDefined();
  });
});
