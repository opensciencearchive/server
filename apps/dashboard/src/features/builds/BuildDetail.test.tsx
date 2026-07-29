import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { BuildDetail } from "./BuildDetail";

describe("BuildDetail", () => {
  it("renders a published build with 3 succeeded components and a complete Published step", async () => {
    renderWithProviders(<BuildDetail buildId="build_8f3a91c2e5" />);

    // Header: build id (mono) + published status
    expect(await screen.findByText("build_8f3a91c2e5")).toBeInTheDocument();
    expect(screen.getByText("published")).toBeInTheDocument();
    expect(screen.getByText("geo-rnaseq-v2")).toBeInTheDocument();

    // All three succeeded components appear with sha256 digests.
    const table = screen.getByRole("table");
    expect(within(table).getByText("geo-ingester")).toBeInTheDocument();
    expect(within(table).getByText("validate-metadata")).toBeInTheDocument();
    expect(within(table).getByText("resolve-ontology")).toBeInTheDocument();

    // Three succeeded status badges in the component table.
    const succeeded = within(table).getAllByText("succeeded");
    expect(succeeded).toHaveLength(3);

    // Truncated sha256 digests are shown (sha256:XXXX…XXXX).
    const digests = within(table).getAllByText(/^sha256:.+….+$/);
    expect(digests).toHaveLength(3);

    // Pipeline: Published step is marked complete.
    const publishedStep = screen.getByTestId("pipeline-step-published");
    expect(publishedStep).toHaveAttribute("data-state", "complete");
  });

  it("renders a failed build verbatim: ImportError, danger normalise-counts row, sibling explanation", async () => {
    renderWithProviders(<BuildDetail buildId="build_f4a1e6d902" />);

    expect(await screen.findByText("build_f4a1e6d902")).toBeInTheDocument();
    expect(screen.getByText("build_failed")).toBeInTheDocument();

    // Parent error message shown verbatim.
    expect(
      screen.getByText(
        /component 'normalise-counts' failed: ImportError: cannot import name 'SchemaRegistry' from 'osa_sdk\.schema'/,
      ),
    ).toBeInTheDocument();

    // The failed component's own verbatim error appears beneath its name.
    const table = screen.getByRole("table");
    expect(
      within(table).getByText(
        /ImportError: cannot import name 'SchemaRegistry' from 'osa_sdk\.schema' \(\/app\/osa_sdk\/schema\/__init__\.py\)/,
      ),
    ).toBeInTheDocument();

    // The failed row is danger-toned.
    const failedName = within(table).getByText("normalise-counts");
    const row = failedName.closest("tr")!;
    expect(row.className).toMatch(/danger/i);

    // The cancelled sibling is explained as a consequence, not a second problem.
    expect(
      within(table).getByText(/cancelled — a sibling component failed/i),
    ).toBeInTheDocument();

    // Building step is failed; not rendered as a cancellation banner.
    const buildingStep = screen.getByTestId("pipeline-step-building");
    expect(buildingStep).toHaveAttribute("data-state", "failed");
    expect(screen.queryByText(/Cancelled by/i)).not.toBeInTheDocument();
  });

  it("renders a cancelled build as a neutral cause banner with no error styling", async () => {
    renderWithProviders(<BuildDetail buildId="build_c9a8b7c6d5" />);

    expect(await screen.findByText("build_c9a8b7c6d5")).toBeInTheDocument();
    // Header status badge + both cancelled component badges.
    expect(screen.getAllByText("cancelled").length).toBeGreaterThanOrEqual(1);

    // Neutral banner: "Cancelled by system" + verbatim reason.
    const banner = screen.getByTestId("cancel-banner");
    expect(banner).toHaveTextContent(/Cancelled by system/i);
    expect(banner).toHaveTextContent("superseded by build_8f3a91c2e5");

    // Never styled as an error / failure.
    expect(banner.className).not.toMatch(/danger/i);
    expect(screen.queryByText(/build_failed|publish_failed/)).not.toBeInTheDocument();

    // Components just show cancelled — no "sibling failed" explanation.
    expect(
      screen.queryByText(/a sibling component failed/i),
    ).not.toBeInTheDocument();
  });
});
