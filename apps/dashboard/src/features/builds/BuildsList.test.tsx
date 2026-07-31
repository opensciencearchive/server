import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { BuildsList } from "./BuildsList";

describe("BuildsList", () => {
  it("renders the archive's builds from the control plane", async () => {
    renderWithProviders(<BuildsList archiveId="arch_a1p1n3c11m" />);

    expect(await screen.findByText("build_8f3a91c2e5")).toBeInTheDocument();
    const table = screen.getByRole("table");
    expect(within(table).getAllByText("geo-rnaseq-v2").length).toBeGreaterThan(
      0,
    );
    expect(screen.queryByText(/sample data/i)).not.toBeInTheDocument();
  });

  it("tells the reader an archive with no builds is empty, not broken", async () => {
    renderWithProviders(<BuildsList archiveId="arch_st0pp3dxx1" />);

    expect(await screen.findByText(/no builds yet/i)).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });
});
