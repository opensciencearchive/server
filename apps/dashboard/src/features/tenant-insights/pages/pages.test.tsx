import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { AuthenticationPanel } from "./AuthenticationPanel";
import { RecordsPanel } from "./RecordsPanel";
import { ValidationPanel } from "./ValidationPanel";

const ARCHIVE_ID = "arch_c0r73xa71a";

describe("RecordsPanel", () => {
  it("marks the section as sample data and renders a seeded record row", async () => {
    renderWithProviders(<RecordsPanel archiveId={ARCHIVE_ID} />);

    expect(await screen.findByText("Sample data")).toBeInTheDocument();
    expect(
      await screen.findByText("Prefrontal cortex snRNA-seq, donor H-1042"),
    ).toBeInTheDocument();
    expect(screen.getByText("srn:rec:9f2c4a1e")).toBeInTheDocument();
  });
});

describe("ValidationPanel", () => {
  it("tints the failing check row with a warning tone", async () => {
    renderWithProviders(<ValidationPanel archiveId={ARCHIVE_ID} />);

    const cell = await screen.findByText("Assay metadata complete");
    const row = cell.closest("tr")!;
    // The failing check (failing > 0) carries the warning row tone.
    expect(row.className).toMatch(/tone-warning/);
    expect(row).toHaveTextContent("12");
  });
});

describe("AuthenticationPanel", () => {
  it("shows the REAL archive admins and links rotation to settings", async () => {
    renderWithProviders(<AuthenticationPanel archiveId={ARCHIVE_ID} />);

    // Admins come from the real archive config (arch_c0r73xa71a), not the mock.
    expect(await screen.findByText("0000-0002-1825-0097")).toBeInTheDocument();
    expect(screen.getByText("0000-0001-5109-3700")).toBeInTheDocument();

    const settingsLink = screen.getByRole("link", { name: /settings/i });
    expect(settingsLink).toHaveAttribute(
      "href",
      `/archives/${ARCHIVE_ID}/settings`,
    );
  });
});
