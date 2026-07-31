import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";

import { AuthenticationPanel } from "./AuthenticationPanel";
import { RecordsPanel } from "./RecordsPanel";

const ARCHIVE_ID = "arch_a1p1n3c11m";

describe("RecordsPanel", () => {
  it("marks the section as sample data and renders a seeded record row", async () => {
    renderWithProviders(<RecordsPanel archiveId={ARCHIVE_ID} />);

    // Platform build → MockOSAService → sample data chip.
    expect(await screen.findByText("Sample data")).toBeInTheDocument();
    // A record id and a metadata-field preview from the mock.
    expect(await screen.findByText("9f2c4a1e")).toBeInTheDocument();
    expect(
      screen.getByText(/title: Col du Lac surface timeseries/),
    ).toBeInTheDocument();
  });
});

describe("AuthenticationPanel", () => {
  it("shows the configured admins and flags sample data in demo/mock mode", async () => {
    // Default test services use MockOSAService (tenantDataIsSample), so the
    // panel renders the sample admins with the sample-data chip.
    renderWithProviders(<AuthenticationPanel archiveId={ARCHIVE_ID} />);

    expect(await screen.findByText("0000-0002-1825-0097")).toBeInTheDocument();
    expect(screen.getByText("0000-0001-5109-3700")).toBeInTheDocument();
    expect(screen.getByText("Sample data")).toBeInTheDocument();
  });
});

describe("sample-data chip gating", () => {
  it("shows real records and NO sample-data affordance off the platform (self-host)", async () => {
    renderWithProviders(<RecordsPanel archiveId={ARCHIVE_ID} />, {
      services: {
        ...makeTestServices(),
        isPlatform: false,
        tenantDataIsSample: false,
      },
    });

    // Records still render, but there is no sample-data chip or note.
    expect(await screen.findByText("9f2c4a1e")).toBeInTheDocument();
    expect(screen.queryByText("Sample data")).not.toBeInTheDocument();
  });

  it("shows NO chip for a platform archive served by the real read-proxy", async () => {
    // Platform build, but tenant reads are wired to the real control-plane
    // read-proxy (#185) — data is real, so no sample affordance.
    renderWithProviders(<RecordsPanel archiveId={ARCHIVE_ID} />, {
      services: { ...makeTestServices(), tenantDataIsSample: false },
    });

    expect(await screen.findByText("9f2c4a1e")).toBeInTheDocument();
    expect(screen.queryByText("Sample data")).not.toBeInTheDocument();
  });

  it("shows the auth config with NO chip for a real archive (auth is a read surface now)", async () => {
    // Auth config is read through the same proxy as every other surface
    // (#184/#185), so a real archive shows it for real — no sample affordance.
    renderWithProviders(<AuthenticationPanel archiveId={ARCHIVE_ID} />, {
      services: { ...makeTestServices(), tenantDataIsSample: false },
    });

    expect(await screen.findByText("0000-0002-1825-0097")).toBeInTheDocument();
    expect(screen.queryByText("Sample data")).not.toBeInTheDocument();
  });
});
