import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MockAmacrinService } from "@/api/amacrin/mock";
import { ApiError } from "@/api/http/errors";
import { makeTestServices, renderWithProviders } from "@/test/render";

import { DeploymentHistory } from "./DeploymentHistory";

describe("DeploymentHistory", () => {
  it("lists the archive's deployments with the OSA version each provisioned", async () => {
    renderWithProviders(<DeploymentHistory archiveId="arch_a1p1n3c11m" />);

    const table = await screen.findByRole("table");
    expect(within(table).getByText("succeeded")).toBeInTheDocument();
    expect(within(table).getByText("v0.0.9")).toBeInTheDocument();
  });

  it("shows the failure message on a failed deployment", async () => {
    renderWithProviders(<DeploymentHistory archiveId="arch_so1lmo1s7r" />);

    expect(
      await screen.findByText(/PersistentVolumeClaim pending after 600s/),
    ).toBeInTheDocument();
  });

  it("distinguishes a failed request from an archive with no deployments", async () => {
    const amacrin = new MockAmacrinService();
    amacrin.listDeployments = () =>
      Promise.reject(
        new ApiError({ status: 503, code: "unavailable", message: "nope" }),
      );

    renderWithProviders(<DeploymentHistory archiveId="arch_a1p1n3c11m" />, {
      services: makeTestServices({ amacrin }),
    });

    expect(await screen.findByText(/history unavailable/i)).toBeInTheDocument();
    expect(
      screen.queryByText(/no deployments recorded/i),
    ).not.toBeInTheDocument();
  });
});
