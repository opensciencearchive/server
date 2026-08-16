import { HttpResponse, http } from "msw";
import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { server } from "@/mocks/server";
import { makeTestServices, renderWithProviders } from "@/test/render";

import { SelfHostVersion } from "./SelfHostVersion";

function nodeOverview(over?: Record<string, unknown>) {
  return {
    name: "Pockets",
    domain: "pockets.example.org",
    description: "",
    osaVersion: "0.0.11",
    status: "ready",
    records: 1,
    schemas: 1,
    latestOsaVersion: "v0.0.12",
    ...over,
  };
}

function selfHost() {
  const services = makeTestServices();
  return { ...services, isPlatform: false as const };
}

describe("SelfHostVersion", () => {
  it("shows the CLI command when the node is behind the latest release", async () => {
    server.use(
      http.get("*/api/node", () => HttpResponse.json(nodeOverview())),
    );
    renderWithProviders(<SelfHostVersion />, { services: selfHost() });

    expect(
      await screen.findByText("Update available → v0.0.12"),
    ).toBeInTheDocument();
    // The bare `0.0.11` from /health compares correctly against `v0.0.12`.
    expect(
      screen.getByText("osa start --osa-version v0.0.12"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /upgrade/i }),
    ).not.toBeInTheDocument();
  });

  it("shows up-to-date when current matches latest despite prefix mismatch", async () => {
    server.use(
      http.get("*/api/node", () =>
        HttpResponse.json(nodeOverview({ latestOsaVersion: "v0.0.11" })),
      ),
    );
    renderWithProviders(<SelfHostVersion />, { services: selfHost() });
    expect(await screen.findByText("Up to date")).toBeInTheDocument();
  });

  it("renders just the version when the registry check failed", async () => {
    server.use(
      http.get("*/api/node", () =>
        HttpResponse.json(nodeOverview({ latestOsaVersion: null })),
      ),
    );
    renderWithProviders(<SelfHostVersion />, { services: selfHost() });
    expect(await screen.findByText("0.0.11")).toBeInTheDocument();
    expect(screen.queryByText(/Update available/)).not.toBeInTheDocument();
    expect(screen.queryByText("Up to date")).not.toBeInTheDocument();
  });
});
